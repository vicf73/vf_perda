# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import bcrypt
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import utils

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÃO PARA BANCO DE DATOS (STREAMLIT SECRETS) ---
# Usar st.secrets para configurações sensíveis
POSTGRES_CONFIG = {
    'host': st.secrets["postgres"]["host"],
    'port': st.secrets["postgres"]["port"],
    'database': st.secrets["postgres"]["database"],
    'user': st.secrets["postgres"]["user"],
    'password': st.secrets["postgres"]["password"]
}
# Neon requer SSL e usamos psycopg2 explicitamente para consistência
POSTGRES_URL = (
    f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@"
    f"{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}?sslmode=require"
)
logger.info(f"Configuração carregada com sucesso para o banco em: {POSTGRES_CONFIG['host']}")

class PostgresDatabaseManager:
    """Gerencia a conexão e operações com o banco de dados PostgreSQL, 
    incluindo autenticação segura (bcrypt) e operações de dados otimizadas.
    """
    
    # Mapeamentos centralizados para evitar inconsistências
    MAPEAMENTO_COLUNAS = {
        'est_ctr': 'est_contr',
        'desc_tp_cli': 'desc_tp_cli',
        'criterio': 'criterio',
        'anomalia': 'anomalia',
        'sit_div': 'sit_div',
        'est_inspec': 'est_inspec',
        'desv': 'desv'
    }
    
    MAPEAMENTO_CRITERIOS = {
        "Criterio": "criterio",
        "Anomalia": "anomalia", 
        "DESC_TP_CLI": "desc_tp_cli",
        "EST_CTR": "est_contr",
        "sit_div": "sit_div",
        "desv": "desv",
        "est_inspec": "est_inspec" 
    }
    
    def __init__(self, database_url):
        self.database_url = database_url
        self.engine = None
        
        try:
            # Configurações de conexão otimizadas para o ambiente de produção
            self.engine = create_engine(
                database_url,
                pool_pre_ping=True,
                pool_recycle=3600,
                pool_size=5,
                max_overflow=10,
                connect_args={
                    'connect_timeout': 15
                }
            )
            
            # Testar conexão
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                logger.info("✅ Conexão com o Banco de Dados estabelecida com sucesso")
                
            self.init_db()
            
        except Exception as e:
            error_msg = f"❌ Erro ao conectar com o Banco de Dados: {str(e)}"
            st.error(error_msg)
            logger.error(error_msg)
            
            # Mensagem geral para o usuário final
            st.error("""
            🔌 **Erro de Conexão com o Servidor**
            
            Não foi possível estabelecer uma conexão estável com o banco de dados. 
            Por favor, tente novamente em alguns instantes ou entre em contato com o administrador.
            """)
                
            raise

    def _get_conn(self):
        """Retorna uma conexão ativa com o banco."""
        return self.engine.connect()

    # --- Inicialização e Estrutura do BD ---
    def init_db(self):
        """Cria as tabelas 'bd' e 'usuarios' e insere usuários padrão se necessário."""
        with self.engine.connect() as conn:
            # Tabela BD
            conn.execute(text('''
                CREATE TABLE IF NOT EXISTS bd (
                    cil TEXT, prod TEXT, contador TEXT, leitura TEXT, mat_contador TEXT,
                    med_fat TEXT, qtd DOUBLE PRECISION, valor DOUBLE PRECISION, situacao TEXT, acordo TEXT,
                    nib TEXT, seq TEXT, localidade TEXT, pt TEXT, desv TEXT,
                    mat_leitura TEXT, desc_uni TEXT, est_contr TEXT, anomalia TEXT, id TEXT,
                    produto TEXT, nome TEXT, criterio TEXT, desc_tp_cli TEXT, tip TEXT,
                    sit_div TEXT, modelo TEXT, lat DOUBLE PRECISION, long DOUBLE PRECISION, est_inspec TEXT,
                    estado TEXT
                )
            '''))
            
            # Tabela de usuários
            conn.execute(text('''
                CREATE TABLE IF NOT EXISTS usuarios (
                    id SERIAL PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    nome TEXT NOT NULL,
                    role TEXT NOT NULL,
                    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            '''))
            
            # Tabela de logs de geração
            conn.execute(text('''
                CREATE TABLE IF NOT EXISTS log_geracao (
                    id SERIAL PRIMARY KEY,
                    usuario TEXT,
                    tipo TEXT,
                    valor TEXT,
                    criterio TEXT,
                    quantidade_folhas INTEGER,
                    quantidade_registros INTEGER,
                    data_geracao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            '''))
            
            # Inserir usuários padrão se a tabela estiver vazia
            result = conn.execute(text("SELECT COUNT(*) FROM usuarios"))
            count = result.scalar()
            if count == 0:
                usuarios_padrao = [
                    ('Admin', self.hash_password('admin123'), 'Administrador Principal', 'Administrador'),
                    ('AssAdm', self.hash_password('adm123'), 'Assistente Administrativo', 'Assistente Administrativo')
                ]
                for user in usuarios_padrao:
                    conn.execute(
                        text("INSERT INTO usuarios (username, password_hash, nome, role) VALUES (:username, :password_hash, :nome, :role)"),
                        {"username": user[0], "password_hash": user[1], "nome": user[2], "role": user[3]}
                    )
                logger.info("Usuários padrão inseridos na inicialização")
            conn.commit()

    # --- Funções de Hashing e Autenticação (bcrypt) ---
    @staticmethod
    def hash_password(password):
        """Gera um hash seguro da senha usando bcrypt."""
        if not password or len(password.strip()) == 0:
            raise ValueError("Senha não pode ser vazia")
        # O salt é gerado automaticamente pelo bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        return hashed.decode('utf-8')

    def autenticar_usuario(self, username, password):
        """Verifica as credenciais do usuário usando bcrypt."""
        if not username or not password:
            return None
            
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT id, username, password_hash, nome, role FROM usuarios WHERE username = :username"),
                {"username": username.strip()}
            )
            usuario = result.fetchone()
        
        if usuario:
            try:
                if bcrypt.checkpw(password.encode('utf-8'), usuario[2].encode('utf-8')):
                    logger.info(f"Autenticação bem-sucedida para: {username}")
                    return {'id': usuario[0], 'username': usuario[1], 'nome': usuario[3], 'role': usuario[4]}
            except (ValueError, Exception) as e:
                logger.warning(f"Hash inválido ou erro na autenticação para {username}: {e}")
                return None 
        logger.warning(f"Tentativa de autenticação falhou para: {username}")
        return None

    # --- Funções de Gerenciamento de Usuários ---
    def obter_usuarios(self):
        """Retorna a lista de todos os usuários."""
        with self.engine.connect() as conn:
            df = pd.read_sql_query(text("SELECT id, username, nome, role, data_criacao FROM usuarios ORDER BY username"), conn)
        return df.to_records(index=False).tolist()

    def validar_dados_usuario(self, username, password, nome, role):
        """Valida dados do usuário antes de criar/editar."""
        errors = []
        if not username or len(username.strip()) < 3:
            errors.append("Nome de usuário deve ter pelo menos 3 caracteres")
        if password and len(password) < 6:
            errors.append("Senha deve ter pelo menos 6 caracteres")
        if not nome or len(nome.strip()) < 2:
            errors.append("Nome completo é obrigatório")
        if role not in ['Administrador', 'Assistente Administrativo', 'Técnico']:
            errors.append("Função inválida")
        return errors

    def criar_usuario(self, username, password, nome, role):
        """Cria um novo usuário com validação."""
        validation_errors = self.validar_dados_usuario(username, password, nome, role)
        if validation_errors:
            return False, " | ".join(validation_errors)
            
        try:
            password_hash = self.hash_password(password)
            with self.engine.connect() as conn:
                conn.execute(
                    text("INSERT INTO usuarios (username, password_hash, nome, role) VALUES (:username, :password_hash, :nome, :role)"),
                    {"username": username.strip(), "password_hash": password_hash, "nome": nome.strip(), "role": role}
                )
                conn.commit()
            logger.info(f"Usuário {username} criado com sucesso")
            return True, "Usuário criado com sucesso!"
        except SQLAlchemyError as e:
            if 'duplicate key value violates unique constraint' in str(e):
                logger.warning(f"Tentativa de criar usuário duplicado: {username}")
                return False, f"O nome de usuário '{username}' já existe."
            logger.error(f"Erro ao criar usuário {username}: {e}")
            return False, f"Erro ao criar usuário: {e}"

    def editar_usuario(self, user_id, nome, role):
        """Edita nome e função de um usuário existente."""
        validation_errors = self.validar_dados_usuario("temp", None, nome, role)
        if validation_errors:
            return False, " | ".join([e for e in validation_errors if "usuário" not in e and "senha" not in e])
            
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("UPDATE usuarios SET nome = :nome, role = :role WHERE id = :id"),
                    {"nome": nome.strip(), "role": role, "id": user_id}
                )
                conn.commit()
            if result.rowcount > 0:
                logger.info(f"Usuário ID {user_id} editado com sucesso")
                return True, "Usuário editado com sucesso!"
            else:
                return False, "Usuário não encontrado."
        except SQLAlchemyError as e:
            logger.error(f"Erro ao editar usuário ID {user_id}: {e}")
            return False, f"Erro ao editar usuário: {e}"

    def excluir_usuario(self, user_id):
        """Exclui um usuário pelo ID com validações de segurança."""
        try:
            with self.engine.connect() as conn:
                # Impedir exclusão do usuário admin principal
                result = conn.execute(
                    text("SELECT username FROM usuarios WHERE id = :id"),
                    {"id": user_id}
                )
                usuario = result.fetchone()
                
                if usuario and usuario[0] == 'Admin':
                    return False, "Não é permitido excluir o usuário Administrador Principal."
                
                result = conn.execute(
                    text("DELETE FROM usuarios WHERE id = :id"),
                    {"id": user_id}
                )
                conn.commit()
                
            if result.rowcount > 0:
                logger.info(f"Usuário ID {user_id} excluído com sucesso")
                return True, "Usuário excluído com sucesso!"
            else:
                return False, "Usuário não encontrado."
        except SQLAlchemyError as e:
            logger.error(f"Erro ao excluir usuário ID {user_id}: {e}")
            return False, f"Erro ao excluir usuário: {e}"

    def alterar_senha(self, user_id, new_password):
        """Altera a senha de um usuário existente."""
        if not new_password or len(new_password) < 6:
            return False, "Senha deve ter pelo menos 6 caracteres"
            
        try:
            password_hash = self.hash_password(new_password)
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("UPDATE usuarios SET password_hash = :hash WHERE id = :id"),
                    {"hash": password_hash, "id": user_id}
                )
                conn.commit()
            if result.rowcount > 0:
                logger.info(f"Senha do usuário ID {user_id} alterada com sucesso")
                return True, "Senha alterada com sucesso!"
            else:
                return False, "Usuário não encontrado."
        except SQLAlchemyError as e:
            logger.error(f"Erro ao alterar senha do usuário ID {user_id}: {e}")
            return False, f"Erro ao alterar senha: {e}"

    # --- Funções de Importação e Dados (Otimizadas) ---
    def importar_csv(self, arquivo_csv, tabela='BD', colunas_esperadas=31):
        """Importa dados do CSV para a tabela BD do PostgreSQL, otimizado para grandes volumes."""
        try:
            # 1. Detecção e Leitura
            encoding = utils.detectar_encoding(arquivo_csv)
            separador = utils.detectar_separador(arquivo_csv, encoding)

            if tabela == 'BD':
                df_novo = pd.read_csv(arquivo_csv, sep=separador, encoding=encoding, 
                                      on_bad_lines='skip', header=None, low_memory=False) 
                
                if len(df_novo.columns) < colunas_esperadas:
                    st.error(f"❌ O arquivo BD deve ter pelo menos {colunas_esperadas} colunas. Encontradas: {len(df_novo.columns)}")
                    return False
                
                # Mapeamento de colunas
                column_mapping = {
                    0: 'cil', 1: 'prod', 2: 'contador', 3: 'leitura', 4: 'mat_contador',
                    5: 'med_fat', 6: 'qtd', 7: 'valor', 8: 'situacao', 9: 'acordo',
                    10: 'nib', 11: 'seq', 12: 'localidade', 13: 'pt', 14: 'desv',
                    15: 'mat_leitura', 16: 'desc_uni', 17: 'est_contr', 18: 'anomalia', 19: 'id',
                    20: 'produto', 21: 'nome', 22: 'criterio', 23: 'desc_tp_cli', 24: 'tip',
                    25: 'sit_div', 26: 'modelo', 27: 'lat', 28: 'long', 29: 'est_inspec',
                    30: 'estado'
                }
                
                df_novo.rename(columns=column_mapping, inplace=True)
                
                # 2. Tratamento e Limpeza
                for col in ['criterio', 'pt', 'localidade', 'nib', 'cil', 'estado']:
                    if col in df_novo.columns:
                        df_novo[col] = df_novo[col].fillna('').astype(str).str.strip()

                df_novo['criterio'] = df_novo['criterio'].str.upper()
                df_novo['pt'] = df_novo['pt'].str.upper()
                df_novo['localidade'] = df_novo['localidade'].str.upper()
                df_novo['estado'] = df_novo['estado'].str.lower()
                
                # Tratamento de Numéricos
                df_novo['qtd'] = pd.to_numeric(df_novo['qtd'], errors='coerce').fillna(0)
                df_novo['valor'] = pd.to_numeric(df_novo['valor'], errors='coerce').fillna(0)
                df_novo['lat'] = pd.to_numeric(df_novo['lat'], errors='coerce')
                df_novo['long'] = pd.to_numeric(df_novo['long'], errors='coerce')
                
                # 3. Operações no BD
                with self.engine.connect() as conn:
                    # Criar tabela temporária
                    df_novo.to_sql('bd_temp_import', conn, if_exists='replace', index=False)
                    
                    # Preservar estado 'prog' existente
                    update_query = text("""
                        UPDATE bd_temp_import as new 
                        SET estado = 'prog' 
                        FROM bd as old
                        WHERE new.cil = old.cil AND old.estado = 'prog'
                    """)
                    result = conn.execute(update_query)
                    st.info(f"O estado 'prog' foi preservado para {result.rowcount} registro(s) durante a importação.")
                    
                    # Substituir a tabela BD
                    conn.execute(text("DROP TABLE IF EXISTS bd CASCADE"))
                    conn.execute(text("ALTER TABLE bd_temp_import RENAME TO bd"))
                    conn.commit()

                self.ordenar_tabela_bd()
                logger.info(f"CSV importado com sucesso: {len(df_novo)} registros")
                return True
            
        except Exception as e:
            error_msg = f"❌ Erro ao importar arquivo para PostgreSQL: {str(e)}"
            st.error(error_msg)
            logger.error(error_msg)
            return False

    def ordenar_tabela_bd(self):
        """Placeholder: A ordenação física é desabilitada. A ordenação será feita nas QUERIES."""
        st.info("ℹ️ Ordenação da tabela BD física desabilitada para otimização de performance.")
        return True

    @st.cache_data(ttl=60, show_spinner=False)
    def obter_valores_unicos_com_contagem(_self, coluna, tabela='bd'):
        """Obtém dicionário {valor: count} de registros disponíveis."""
        try:
            with _self.engine.connect() as conn:
                coluna_sql = _self.MAPEAMENTO_COLUNAS.get(coluna.lower(), coluna.lower())
                
                query = text(f"""
                    SELECT UPPER(TRIM({coluna_sql})) as valor, COUNT(*) as qtd
                    FROM {tabela}
                    WHERE {coluna_sql} IS NOT NULL 
                    AND TRIM({coluna_sql}) != '' 
                    AND LOWER(TRIM(estado)) != 'prog'
                    GROUP BY UPPER(TRIM({coluna_sql}))
                    ORDER BY valor
                """)
                
                df = pd.read_sql_query(query, conn)
                return dict(zip(df['valor'], df['qtd']))
        except Exception as e:
            st.error(f"Erro ao obter contagens: {e}")
            return {}

    def obter_historico_geracao(self):
        """Retorna os últimos 20 registros de geração."""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        id, 
                        usuario, 
                        tipo, 
                        valor, 
                        criterio, 
                        quantidade_folhas, 
                        quantidade_registros, 
                        TO_CHAR(data_geracao, 'DD/MM/YYYY HH24:MI') as data_formatada 
                    FROM log_geracao 
                    ORDER BY data_geracao DESC 
                    LIMIT 20
                """)
                return pd.read_sql_query(query, conn)
        except Exception as e:
            logger.error(f"Erro ao obter histórico: {e}")
            return pd.DataFrame()

    @st.cache_data(ttl=3600, show_spinner=False)
    def obter_valores_unicos(_self, coluna, tabela='bd'):
        """Obtém valores únicos de uma coluna, com cache para melhor performance."""
        try:
            with _self.engine.connect() as conn:
                # Usa o nome mapeado ou o original se não estiver no mapeamento
                coluna_sql = _self.MAPEAMENTO_COLUNAS.get(coluna.lower(), coluna.lower())
                    
                query = text(f"""
                    SELECT DISTINCT UPPER(TRIM({coluna_sql})) as valor_unico
                    FROM {tabela} 
                    WHERE {coluna_sql} IS NOT NULL 
                    AND TRIM({coluna_sql}) != '' 
                    AND TRIM(UPPER({coluna_sql})) NOT IN ('NONE', 'NULL')
                    ORDER BY valor_unico
                """)
                
                df = pd.read_sql_query(query, conn)
                valores = df['valor_unico'].tolist()
                logger.debug(f"Valores únicos obtidos para {coluna}: {len(valores)} valores")
                return valores
        except Exception as e:
            st.error(f"❌ Erro ao obter valores únicos para {coluna}: {e}")
            return []

    def gerar_folhas_trabalho(self, tipo_folha, valor_selecionado, quantidade_folhas, quantidade_nibs, cils_validos=None, criterio_tipo=None, criterio_valor=None, user_name=None):
        """Gera folhas de trabalho com filtragem e ordenação no SQL."""
        try:
            with self.engine.connect() as conn:
                
                cils_restantes_nao_encontrados = []
                
                # 1. Construção da Query Otimizada
                cols_to_select = [
                    'cil', 'prod', 'contador', 'leitura', 'mat_contador',
                    'med_fat', 'qtd', 'valor', 'situacao', 'acordo',
                    'nib', 'seq', 'localidade', 'pt', 'desv', 'estado',
                    'criterio', 'anomalia', 'desc_tp_cli'
                ]
                select_clause = f"SELECT {', '.join(cols_to_select)} FROM bd"
                
                query_params = {}
                where_conditions = []

                # Lógica Específica para AVULSO
                if tipo_folha == "AVULSO":
                     # AVULSO: Ignora estado 'prog' e critérios
                     if cils_validos:
                        where_conditions.append("cil = ANY(:cils)")
                        query_params['cils'] = cils_validos
                     else:
                        return None, []
                else:
                    # Padrão: Filtra 'prog' e aplica critérios
                    where_conditions.append("LOWER(TRIM(estado)) != 'prog'")

                    if criterio_tipo and criterio_valor:
                        coluna_criterio = self.MAPEAMENTO_CRITERIOS.get(criterio_tipo)
                        if coluna_criterio:
                            where_conditions.append(f"UPPER(TRIM({coluna_criterio})) = :criterio_valor")
                            query_params['criterio_valor'] = criterio_valor.strip().upper()

                    if valor_selecionado:
                        valor_selecionado_limpo = valor_selecionado.strip().upper()
                        coluna_filtro = 'pt' if tipo_folha == "PT" else 'localidade'
                        where_conditions.append(f"UPPER(TRIM({coluna_filtro})) = :valor_filtro")
                        query_params['valor_filtro'] = valor_selecionado_limpo
                
                # ... (Ordenação)
                order_by_clause = """
                    ORDER BY 
                        CASE WHEN seq IS NULL OR TRIM(seq) = '' THEN 1 ELSE 0 END, seq,
                        CASE WHEN nib IS NULL OR TRIM(nib) = '' THEN 1 ELSE 0 END, nib
                """
                
                where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
                full_query = f"{select_clause} {where_clause} {order_by_clause}"
                
                df = pd.read_sql_query(text(full_query), conn, params=query_params)

                if tipo_folha == "AVULSO" and cils_validos:
                    cils_encontrados = set(df['cil'].unique()) if not df.empty else set()
                    cils_restantes_nao_encontrados = list(set(cils_validos) - cils_encontrados)

                if df.empty:
                    return None, cils_restantes_nao_encontrados
                
                # 3. Geração das Folhas
                df['nib'] = df['nib'].fillna('').astype(str).str.strip()
                nibs_unicos = df['nib'].unique()
                total_nibs = len(nibs_unicos)
                
                if total_nibs == 0:
                    return None, cils_restantes_nao_encontrados
                
                folhas_possiveis = (total_nibs + quantidade_nibs - 1) // quantidade_nibs
                quantidade_folhas = min(quantidade_folhas, folhas_possiveis)
                
                folhas = []
                total_registros_atualizados = 0
                
                for i in range(quantidade_folhas):
                    nibs_na_folha = nibs_unicos[i * quantidade_nibs: (i + 1) * quantidade_nibs].tolist()
                    folha_df = df[df['nib'].isin(nibs_na_folha)].copy()
                    folha_df['FOLHA'] = i + 1
                    folhas.append(folha_df)
                    
                    # 4. Atualização de Estado (APENAS SE NÃO FOR AVULSO)
                    if tipo_folha != "AVULSO":
                        update_where_conditions = ["LOWER(TRIM(estado)) != 'prog'"]
                        update_params = {'nibs': nibs_na_folha}
                        
                        if criterio_tipo and criterio_valor:
                            coluna_criterio = self.MAPEAMENTO_CRITERIOS.get(criterio_tipo)
                            if coluna_criterio:
                                update_where_conditions.append(f"UPPER(TRIM({coluna_criterio})) = :criterio_valor")
                                update_params['criterio_valor'] = criterio_valor.strip().upper()

                        if tipo_folha == "PT" or tipo_folha == "LOCALIDADE":
                            coluna_filtro = 'pt' if tipo_folha == "PT" else 'localidade'
                            update_where_conditions.append(f"UPPER(TRIM({coluna_filtro})) = :valor_update")
                            update_params['valor_update'] = valor_selecionado.strip().upper()
                        
                        update_query = text(f"""
                            UPDATE bd SET estado = 'prog' 
                            WHERE nib = ANY(:nibs) AND {' AND '.join(update_where_conditions)}
                        """)
                        
                        result = conn.execute(update_query, update_params)
                        total_registros_atualizados += result.rowcount
                    else:
                        # Se for Avulso, conta os registros mas não atualiza
                        total_registros_atualizados += len(folha_df)
            
                conn.commit()
                st.success(f"✅ Estado atualizado para 'prog' em {total_registros_atualizados} registros.")
                logger.info(f"Folhas geradas: {quantidade_folhas}, registros atualizados: {total_registros_atualizados}")
                
                # 5. Registrar no log de geração
                try:
                    if user_name:
                        log_query = text("""
                            INSERT INTO log_geracao (usuario, tipo, valor, criterio, quantidade_folhas, quantidade_registros)
                            VALUES (:usuario, :tipo, :valor, :criterio, :qtd_folhas, :qtd_regs)
                        """)
                        log_criterio = f"{criterio_tipo}={criterio_valor}" if criterio_tipo else "Nenhum"
                        log_valor = "Avulso" if tipo_folha == "AVULSO" else valor_selecionado
                        
                        conn.execute(log_query, {
                            'usuario': user_name,
                            'tipo': tipo_folha,
                            'valor': log_valor,
                            'criterio': log_criterio,
                            'qtd_folhas': quantidade_folhas,
                            'qtd_regs': total_registros_atualizados
                        })
                        conn.commit()
                except Exception as log_error:
                    logger.error(f"Erro ao salvar log de geração: {log_error}")

                if folhas:
                    resultado_df = pd.concat(folhas, ignore_index=True)
                    return resultado_df, cils_restantes_nao_encontrados
                else:
                    return None, cils_restantes_nao_encontrados
            
        except Exception as e:
            error_msg = f"❌ Erro ao gerar folhas no Postgres: {str(e)}"
            st.error(error_msg)
            logger.error(error_msg)
            return None, []

    def simular_folhas_trabalho(self, tipo_folha, valor_selecionado, quantidade_folhas, quantidade_nibs, cils_validos=None, criterio_tipo=None, criterio_valor=None):
        """Simula a geração de folhas e retorna estatísticas e preview."""
        try:
            with self.engine.connect() as conn:
                cils_restantes_nao_encontrados = []
                
                # 1. Construção da Query
                cols_to_select = [
                    'cil', 'prod', 'contador', 'leitura', 'mat_contador',
                    'med_fat', 'qtd', 'valor', 'situacao', 'acordo',
                    'nib', 'seq', 'localidade', 'pt', 'desv', 'estado',
                    'criterio', 'anomalia', 'desc_tp_cli'
                ]
                select_clause = f"SELECT {', '.join(cols_to_select)} FROM bd"
                
                query_params = {}
                where_conditions = []

                # Lógica Específica para AVULSO
                if tipo_folha == "AVULSO":
                    # AVULSO: Ignora estado 'prog', ignora critérios, apenas filtra pelos CILs fornecidos
                    if cils_validos:
                         where_conditions.append("cil = ANY(:cils)")
                         query_params['cils'] = cils_validos
                    else:
                         return None # Sem CILs, nada a fazer
                else:
                    # Padrão (PT/LOCALIDADE): Ignora 'prog' e aplica filtros
                    where_conditions.append("LOWER(TRIM(estado)) != 'prog'")
                    
                    if criterio_tipo and criterio_valor:
                        coluna_criterio = self.MAPEAMENTO_CRITERIOS.get(criterio_tipo)
                        if coluna_criterio:
                            where_conditions.append(f"UPPER(TRIM({coluna_criterio})) = :criterio_valor")
                            query_params['criterio_valor'] = criterio_valor.strip().upper()

                    if valor_selecionado:
                        valor_selecionado_limpo = valor_selecionado.strip().upper()
                        coluna_filtro = 'pt' if tipo_folha == "PT" else 'localidade'
                        where_conditions.append(f"UPPER(TRIM({coluna_filtro})) = :valor_filtro")
                        query_params['valor_filtro'] = valor_selecionado_limpo
                
                # Montar WHERE
                where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
                
                # Ordenação simplificada para preview
                full_query = f"{select_clause} {where_clause} LIMIT 1000"
                
                df = pd.read_sql_query(text(full_query), conn, params=query_params)
                
                # ... (restante da lógica de cálculo mantida)
                if tipo_folha == "AVULSO" and cils_validos:
                    cils_encontrados = set(df['cil'].unique()) if not df.empty else set()
                    cils_restantes_nao_encontrados = list(set(cils_validos) - cils_encontrados)

                if df.empty:
                    return {
                        'total_registros': 0,
                        'total_nibs': 0,
                        'folhas_possiveis': 0,
                        'preview_df': pd.DataFrame(),
                        'cils_nao_encontrados': cils_restantes_nao_encontrados
                    }
                
                # Cálculos de estimativa
                df['nib'] = df['nib'].fillna('').astype(str).str.strip()
                nibs_unicos = df['nib'].unique()
                total_nibs = len(nibs_unicos)
                
                folhas_possiveis_total = (total_nibs + quantidade_nibs - 1) // quantidade_nibs
                folhas_a_gerar = min(quantidade_folhas, folhas_possiveis_total)
                
                return {
                    'total_registros': len(df),
                    'total_nibs': total_nibs,
                    'folhas_possiveis': folhas_possiveis_total,
                    'folhas_a_gerar': folhas_a_gerar,
                    'preview_df': df.head(5),
                    'cils_nao_encontrados': cils_restantes_nao_encontrados
                }

        except Exception as e:
            logger.error(f"Erro ao simular folhas: {e}")
            return None

    def resetar_estado(self, tipo, valor):
        """Reseta o estado 'prog' para o tipo e valor selecionados."""
        try:
            with self.engine.connect() as conn:
                valor_sql = valor.strip().upper() if valor else ""
                
                if tipo == 'PT':
                    query = text("UPDATE bd SET estado = '' WHERE LOWER(TRIM(estado)) = 'prog' AND UPPER(TRIM(pt)) = :valor")
                    params = {"valor": valor_sql}
                elif tipo == 'LOCALIDADE':
                    query = text("UPDATE bd SET estado = '' WHERE LOWER(TRIM(estado)) = 'prog' AND UPPER(TRIM(localidade)) = :valor")
                    params = {"valor": valor_sql}
                elif tipo == 'AVULSO':
                    query = text("UPDATE bd SET estado = '' WHERE LOWER(TRIM(estado)) = 'prog'")
                    params = {}
                else:
                    return False, "Tipo de reset inválido."
                    
                result = conn.execute(query, params)
                conn.commit()
                registros_afetados = result.rowcount
                logger.info(f"Reset de estado: {tipo} - {valor}, {registros_afetados} registros afetados")
                return True, registros_afetados
                
        except Exception as e:
            error_msg = f"❌ Erro ao resetar o estado no Postgres: {str(e)}"
            st.error(error_msg)
            logger.error(error_msg)
            return False, 0

    # --- NOVOS MÉTODOS PARA RELATÓRIOS E DASHBOARDS ---
    
    @st.cache_data(ttl=1800, show_spinner=False)
    def obter_estatisticas_gerais(_self):
        """Obtém estatísticas gerais do sistema para dashboard."""
        try:
            with _self.engine.connect() as conn:
                # Estatísticas principais
                stats_query = text("""
                    SELECT 
                        COUNT(*) as total_registros,
                        COUNT(DISTINCT cil) as cils_unicos,
                        COUNT(DISTINCT pt) as pts_unicos,
                        COUNT(DISTINCT localidade) as localidades_unicas,
                        COUNT(DISTINCT nib) as nibs_unicos,
                        SUM(CASE WHEN LOWER(TRIM(estado)) = 'prog' THEN 1 ELSE 0 END) as registros_em_progresso,
                        SUM(qtd) as total_qtd,
                        SUM(valor) as total_valor,
                        AVG(qtd) as media_qtd,
                        AVG(valor) as media_valor
                    FROM bd
                """)
                
                stats_df = pd.read_sql_query(stats_query, conn)
                
                return {
                    'estatisticas_gerais': stats_df.iloc[0].to_dict() if not stats_df.empty else {}
                }
                
        except Exception as e:
            logger.error(f"Erro ao obter estatísticas: {e}")
            return {}
    
    @st.cache_data(ttl=1800, show_spinner=False)
    def obter_metricas_operacionais(_self):
        """Obtém métricas operacionais para relatórios."""
        try:
            with _self.engine.connect() as conn:
                # Eficiência por PT
                eficiencia_pt_query = text("""
                    SELECT 
                        UPPER(TRIM(pt)) as pt,
                        COUNT(*) as total_registros,
                        SUM(CASE WHEN LOWER(TRIM(estado)) = 'prog' THEN 1 ELSE 0 END) as em_progresso,
                        ROUND(SUM(CASE WHEN LOWER(TRIM(estado)) = 'prog' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as percentual_progresso,
                        SUM(valor) as valor_total,
                        AVG(valor) as valor_medio
                    FROM bd
                    WHERE pt IS NOT NULL AND TRIM(pt) != ''
                    GROUP BY UPPER(TRIM(pt))
                    HAVING COUNT(*) > 10
                    ORDER BY total_registros DESC
                    LIMIT 15
                """)
                
                eficiencia_pt_df = pd.read_sql_query(eficiencia_pt_query, conn)
                
                # Top localidades por valor
                top_localidades_query = text("""
                    SELECT 
                        UPPER(TRIM(localidade)) as localidade,
                        COUNT(*) as total_registros,
                        SUM(valor) as valor_total,
                        AVG(valor) as valor_medio
                    FROM bd
                    WHERE localidade IS NOT NULL AND TRIM(localidade) != ''
                    GROUP BY UPPER(TRIM(localidade))
                    ORDER BY valor_total DESC
                    LIMIT 15
                """)
                
                top_localidades_df = pd.read_sql_query(top_localidades_query, conn)
                
                # Distribuição geográfica (com coordenadas)
                geolocalizacao_query = text("""
                    SELECT 
                        lat,
                        long,
                        COUNT(*) as densidade,
                        SUM(valor) as valor_total
                    FROM bd
                    WHERE lat IS NOT NULL AND long IS NOT NULL 
                    AND lat != 0 AND long != 0
                    GROUP BY lat, long
                    HAVING COUNT(*) > 1
                """)
                
                geolocalizacao_df = pd.read_sql_query(geolocalizacao_query, conn)
                
                return {
                    'eficiencia_pt': eficiencia_pt_df.to_dict('records'),
                    'top_localidades': top_localidades_df.to_dict('records'),
                    'geolocalizacao': geolocalizacao_df.to_dict('records')
                }
                
        except Exception as e:
            logger.error(f"Erro ao obter métricas operacionais: {e}")
            return {}

    @st.cache_data(ttl=1800, show_spinner=False)
    def obter_dados_para_dashboard(_self, criterio, valor_filtro=None):
        """Obtém dados específicos para o dashboard baseado no critério selecionado."""
        try:
            with _self.engine.connect() as conn:
                # Mapear o nome do critério para a coluna no banco
                mapeamento_colunas = {
                    'Criterio': 'criterio',
                    'Anomalia': 'anomalia',
                    'EST_CTR': 'est_contr',
                    'sit_div': 'sit_div', 
                    'est_inspec': 'est_inspec',
                    'desv': 'desv'
                }
                
                coluna_sql = mapeamento_colunas.get(criterio, criterio.lower())
                
                # Query base
                query = f"""
                    SELECT 
                        UPPER(TRIM({coluna_sql})) as {criterio.lower()},
                        COUNT(*) as quantidade,
                        SUM(valor) as total_valor,
                        AVG(valor) as valor_medio
                    FROM bd 
                    WHERE {coluna_sql} IS NOT NULL 
                    AND TRIM({coluna_sql}) != ''
                """
                
                params = {}
                
                # Aplicar filtro se especificado
                if valor_filtro and valor_filtro != "Todos":
                    query += f" AND UPPER(TRIM({coluna_sql})) = :valor_filtro"
                    params['valor_filtro'] = valor_filtro.upper().strip()
                
                query += f" GROUP BY UPPER(TRIM({coluna_sql}))"
                
                # Ordenar por quantidade (mais relevante para dashboard)
                query += " ORDER BY quantidade DESC, total_valor DESC"
                
                df_resultado = pd.read_sql_query(text(query), conn, params=params)
                
                return {
                    'distribuicao_criterio': df_resultado.to_dict('records')
                }
                
        except Exception as e:
            logger.error(f"Erro ao obter dados para dashboard ({criterio}): {e}")
            return {}
    
    def gerar_relatorio_detalhado(_self, filtros=None):
        """Gera relatório detalhado com base em filtros."""
        try:
            with _self.engine.connect() as conn:
                base_query = """
                    SELECT 
                        cil, pt, localidade, criterio, anomalia, 
                        situacao, qtd, valor, estado, nib,
                        desc_tp_cli, est_contr, sit_div, est_inspec
                    FROM bd 
                    WHERE 1=1
                """
                
                params = {}
                
                # Aplicar filtros
                if filtros:
                    if filtros.get('criterio'):
                        base_query += " AND UPPER(TRIM(criterio)) = :criterio"
                        params['criterio'] = filtros['criterio'].upper().strip()
                    
                    if filtros.get('pt'):
                        base_query += " AND UPPER(TRIM(pt)) = :pt"
                        params['pt'] = filtros['pt'].upper().strip()
                    
                    if filtros.get('localidade'):
                        base_query += " AND UPPER(TRIM(localidade)) = :localidade"
                        params['localidade'] = filtros['localidade'].upper().strip()
                    
                    if filtros.get('estado'):
                        base_query += " AND LOWER(TRIM(estado)) = :estado"
                        params['estado'] = filtros['estado'].lower().strip()
                
                base_query += " ORDER BY pt, localidade, criterio"
                
                df = pd.read_sql_query(text(base_query), conn, params=params)
                return df
                
        except Exception as e:
            logger.error(f"Erro ao gerar relatório detalhado: {e}")
            return pd.DataFrame()
