# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import datetime
import io
import chardet
import bcrypt
import os
import re
import logging
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO
from zipfile import ZipFile

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from sqlalchemy import create_engine, text, inspect
    from sqlalchemy.exc import SQLAlchemyError
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    st.error("❌ SQLAlchemy não está instalado. Instale com: pip install sqlalchemy psycopg2-binary")
    st.stop()
    
# --- Configuração Segura das Credenciais do Banco de Dados ---

try:
    POSTGRES_CONFIG = {
        'host': st.secrets["postgres"]["host"],
        'port': st.secrets["postgres"]["port"],
        'database': st.secrets["postgres"]["database"],
        'user': st.secrets["postgres"]["user"],
        'password': st.secrets["postgres"]["password"]
    }
except (KeyError, Exception) as e:
    st.error("❌ Erro ao carregar as credenciais do banco de dados. Verifique o arquivo de segredos do Streamlit.")
    logger.error(f"Erro nas credenciais do banco: {e}")
    st.stop()

# Construção segura da URL de conexão
POSTGRES_URL = (
    f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@"
    f"{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}?sslmode=require"
)

# --- DatabaseManager para PostgreSQL ---
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
            self.engine = create_engine(database_url, pool_pre_ping=True, pool_recycle=3600)
            self.init_db()
            logger.info("Conexão com PostgreSQL estabelecida com sucesso")
        except Exception as e:
            error_msg = f"❌ Erro ao conectar com PostgreSQL: {str(e)}"
            st.error(error_msg)
            logger.error(error_msg)
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

    # --- Funções Auxiliares de CSV ---
    def _detectar_encoding(self, arquivo_csv):
        """Detecta o encoding do arquivo."""
        raw_data = arquivo_csv.getvalue()
        result = chardet.detect(raw_data)
        encoding = result['encoding'] or 'utf-8'
        logger.info(f"Encoding detectado: {encoding} (confiança: {result['confidence']})")
        return encoding

    def _detectar_separador(self, arquivo_csv, encoding):
        """Detecta o separador mais provável (',' ou ';')."""
        arquivo_csv.seek(0)
        try:
            amostra = arquivo_csv.read(1024 * 50).decode(encoding, errors='ignore')
            
            virgula_count = amostra.count(',')
            ponto_virgula_count = amostra.count(';')
            
            if ponto_virgula_count > virgula_count * 2:
                separador = ';'
            else:
                separador = ','
                
            logger.info(f"Separador detectado: '{separador}' (;: {ponto_virgula_count}, ,: {virgula_count})")
            return separador
        finally:
            arquivo_csv.seek(0)
    
    # --- Funções de Importação e Dados (Otimizadas) ---
    def importar_csv(self, arquivo_csv, tabela='BD', colunas_esperadas=31):
        """Importa dados do CSV para a tabela BD do PostgreSQL, otimizado para grandes volumes."""
        try:
            # 1. Detecção e Leitura
            encoding = self._detectar_encoding(arquivo_csv)
            separador = self._detectar_separador(arquivo_csv, encoding)

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

    def gerar_folhas_trabalho(self, tipo_folha, valor_selecionado, quantidade_folhas, quantidade_nibs, cils_validos=None, criterio_tipo=None, criterio_valor=None):
        """Gera folhas de trabalho com filtragem e ordenação no SQL."""
        try:
            with self.engine.connect() as conn:
                
                cils_restantes_nao_encontrados = []
                
                # 1. Construção da Query
                select_clause = "SELECT * FROM bd"
                where_conditions = ["LOWER(TRIM(estado)) != 'prog'"]
                query_params = {}
                
                # Adicionar critério de seleção
                if criterio_tipo and criterio_valor:
                    coluna_criterio = self.MAPEAMENTO_CRITERIOS.get(criterio_tipo)
                    if coluna_criterio:
                        where_conditions.append(f"UPPER(TRIM({coluna_criterio})) = :criterio_valor")
                        query_params['criterio_valor'] = criterio_valor.strip().upper()

                # Condições específicas por tipo de folha
                if tipo_folha == "AVULSO" and cils_validos:
                    where_conditions.append("cil = ANY(:cils)")
                    query_params['cils'] = cils_validos
                elif valor_selecionado:
                    valor_selecionado_limpo = valor_selecionado.strip().upper()
                    coluna_filtro = 'pt' if tipo_folha == "PT" else 'localidade'
                    where_conditions.append(f"UPPER(TRIM({coluna_filtro})) = :valor_filtro")
                    query_params['valor_filtro'] = valor_selecionado_limpo
                
                # 2. Ordenação
                order_by_clause = """
                    ORDER BY 
                        CASE WHEN seq IS NULL OR TRIM(seq) = '' THEN 1 ELSE 0 END, seq,
                        CASE WHEN nib IS NULL OR TRIM(nib) = '' THEN 1 ELSE 0 END, nib
                """
                full_query = f"{select_clause} WHERE {' AND '.join(where_conditions)} {order_by_clause}"
                
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
                    
                    # 4. Atualização de Estado
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
            
                conn.commit()
                st.success(f"✅ Estado atualizado para 'prog' em {total_registros_atualizados} registros.")
                logger.info(f"Folhas geradas: {quantidade_folhas}, registros atualizados: {total_registros_atualizados}")
                
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
                
                # Distribuição por critério
                criterio_query = text("""
                    SELECT 
                        UPPER(TRIM(criterio)) as criterio,
                        COUNT(*) as quantidade,
                        SUM(valor) as total_valor
                    FROM bd 
                    WHERE criterio IS NOT NULL AND TRIM(criterio) != ''
                    GROUP BY UPPER(TRIM(criterio))
                    ORDER BY quantidade DESC
                    LIMIT 10
                """)
                
                criterio_df = pd.read_sql_query(criterio_query, conn)
                
                # Distribuição por anomalia
                anomalia_query = text("""
                    SELECT 
                        UPPER(TRIM(anomalia)) as anomalia,
                        COUNT(*) as quantidade
                    FROM bd 
                    WHERE anomalia IS NOT NULL AND TRIM(anomalia) != ''
                    GROUP BY UPPER(TRIM(anomalia))
                    ORDER BY quantidade DESC
                    LIMIT 10
                """)
                
                anomalia_df = pd.read_sql_query(anomalia_query, conn)
                
                return {
                    'estatisticas_gerais': stats_df.iloc[0].to_dict() if not stats_df.empty else {},
                    'distribuicao_criterio': criterio_df.to_dict('records'),
                    'distribuicao_anomalia': anomalia_df.to_dict('records')
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

# --- FUNÇÕES PARA DASHBOARDS E RELATÓRIOS ---

def mostrar_dashboard_geral(db_manager):
    """Dashboard geral com métricas e visualizações."""
    st.markdown("## 📊 Dashboard Geral - Métricas do Sistema")
    
    # Obter dados
    with st.spinner("Carregando dados do dashboard..."):
        estatisticas = db_manager.obter_estatisticas_gerais()
        metricas = db_manager.obter_metricas_operacionais()
    
    if not estatisticas:
        st.error("❌ Não foi possível carregar os dados do dashboard.")
        return
    
    stats = estatisticas['estatisticas_gerais']
    
    # Métricas Principais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total de Registros",
            value=f"{stats.get('total_registros', 0):,}",
            delta=None
        )
    
    with col2:
        st.metric(
            label="CILs Únicos",
            value=f"{stats.get('cils_unicos', 0):,}",
            delta=None
        )
    
    with col3:
        progresso_percent = (stats.get('registros_em_progresso', 0) / max(stats.get('total_registros', 1), 1)) * 100
        st.metric(
            label="Em Progresso",
            value=f"{stats.get('registros_em_progresso', 0):,}",
            delta=f"{progresso_percent:.1f}%"
        )
    
    with col4:
        st.metric(
            label="Valor Total",
            value=f"R$ {stats.get('total_valor', 0):,.2f}",
            delta=None
        )
    
    st.markdown("---")
    
    # Gráficos e Visualizações
    col_left, col_right = st.columns(2)
    
    with col_left:
        # Gráfico de Distribuição por Critério
        if estatisticas['distribuicao_criterio']:
            df_criterio = pd.DataFrame(estatisticas['distribuicao_criterio'])
            fig_criterio = px.pie(
                df_criterio, 
                values='quantidade', 
                names='criterio',
                title='Distribuição por Critério',
                hole=0.4
            )
            st.plotly_chart(fig_criterio, use_container_width=True)
        else:
            st.info("ℹ️ Sem dados de critério para exibir")
    
    with col_right:
        # Gráfico de Distribuição por Anomalia
        if estatisticas['distribuicao_anomalia']:
            df_anomalia = pd.DataFrame(estatisticas['distribuicao_anomalia'])
            fig_anomalia = px.bar(
                df_anomalia,
                x='anomalia',
                y='quantidade',
                title='Top Anomalias',
                color='quantidade'
            )
            fig_anomalia.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_anomalia, use_container_width=True)
        else:
            st.info("ℹ️ Sem dados de anomalia para exibir")
    
    # Mapa de Calor Geográfico
    st.markdown("### 🗺️ Densidade Geográfica")
    if metricas.get('geolocalizacao'):
        df_geo = pd.DataFrame(metricas['geolocalizacao'])
        if not df_geo.empty and len(df_geo) > 1:
            # Usar coordenadas médias como centro do mapa
            lat_center = df_geo['lat'].mean()
            lon_center = df_geo['long'].mean()
            
            fig_mapa = px.density_mapbox(
                df_geo,
                lat='lat',
                lon='long',
                z='densidade',
                radius=20,
                center=dict(lat=lat_center, lon=lon_center),
                zoom=10,
                mapbox_style="open-street-map",
                title="Densidade de Registros por Localização"
            )
            st.plotly_chart(fig_mapa, use_container_width=True)
        else:
            st.info("ℹ️ Dados geográficos insuficientes para exibir o mapa")
    else:
        st.info("ℹ️ Sem dados de geolocalização disponíveis")

def mostrar_relatorio_operacional(db_manager):
    """Relatório operacional detalhado."""
    st.markdown("## 📈 Relatório Operacional")
    
    # Filtros
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        criterios = db_manager.obter_valores_unicos('criterio')
        filtro_criterio = st.selectbox("Filtrar por Critério:", [""] + (criterios if criterios else []))
    
    with col2:
        pts = db_manager.obter_valores_unicos('pt')
        filtro_pt = st.selectbox("Filtrar por PT:", [""] + (pts if pts else []))
    
    with col3:
        localidades = db_manager.obter_valores_unicos('localidade')
        filtro_localidade = st.selectbox("Filtrar por Localidade:", [""] + (localidades if localidades else []))
    
    with col4:
        estados = ["", "prog", ""]
        filtro_estado = st.selectbox("Filtrar por Estado:", estados)
    
    # Aplicar filtros
    filtros = {}
    if filtro_criterio and filtro_criterio != "":
        filtros['criterio'] = filtro_criterio
    if filtro_pt and filtro_pt != "":
        filtros['pt'] = filtro_pt
    if filtro_localidade and filtro_localidade != "":
        filtros['localidade'] = filtro_localidade
    if filtro_estado and filtro_estado != "":
        filtros['estado'] = filtro_estado
    
    if st.button("🔄 Gerar Relatório", type="primary"):
        with st.spinner("Gerando relatório..."):
            df_relatorio = db_manager.gerar_relatorio_detalhado(filtros)
            
        if not df_relatorio.empty:
            st.success(f"✅ Relatório gerado com {len(df_relatorio)} registros")
            
            # Métricas do relatório
            total_valor = df_relatorio['valor'].sum()
            media_valor = df_relatorio['valor'].mean()
            registros_prog = len(df_relatorio[df_relatorio['estado'] == 'prog'])
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Total do Relatório", f"R$ {total_valor:,.2f}")
            col2.metric("Valor Médio", f"R$ {media_valor:,.2f}")
            col3.metric("Em Progresso", registros_prog)
            
            # Tabela de dados
            st.dataframe(df_relatorio, use_container_width=True)
            
            # Opção de download
            csv = df_relatorio.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"relatorio_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.warning("⚠️ Nenhum dado encontrado com os filtros aplicados")

def mostrar_analise_eficiencia(db_manager):
    """Análise de eficiência por PT e Localidade."""
    st.markdown("## 📊 Análise de Eficiência")
    
    with st.spinner("Carregando métricas de eficiência..."):
        metricas = db_manager.obter_metricas_operacionais()
    
    if not metricas.get('eficiencia_pt'):
        st.info("ℹ️ Sem dados de eficiência disponíveis")
        return
    
    df_eficiencia = pd.DataFrame(metricas['eficiencia_pt'])
    
    # Gráfico de eficiência
    fig_eficiencia = px.bar(
        df_eficiencia.head(10),
        x='pt',
        y='percentual_progresso',
        title='Top 10 PTs por Percentual em Progresso',
        color='percentual_progresso',
        labels={'percentual_progresso': '% em Progresso', 'pt': 'PT'}
    )
    fig_eficiencia.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig_eficiencia, use_container_width=True)
    
    # Tabela detalhada
    st.markdown("### 📋 Detalhamento por PT")
    st.dataframe(
        df_eficiencia[['pt', 'total_registros', 'em_progresso', 'percentual_progresso', 'valor_total']],
        use_container_width=True
    )
    
    # Análise por localidade
    if metricas.get('top_localidades'):
        st.markdown("### 🏙️ Top Localidades por Valor")
        df_localidades = pd.DataFrame(metricas['top_localidades'])
        
        fig_localidades = px.treemap(
            df_localidades.head(8),
            path=['localidade'],
            values='valor_total',
            title='Distribuição de Valor por Localidade (Top 8)'
        )
        st.plotly_chart(fig_localidades, use_container_width=True)

def mostrar_relatorio_usuarios(db_manager):
    """Relatório de atividade de usuários."""
    st.markdown("## 👥 Relatório de Usuários")
    
    try:
        usuarios = db_manager.obter_usuarios()
        
        if usuarios:
            df_usuarios = pd.DataFrame(usuarios, columns=['ID', 'Username', 'Nome', 'Role', 'Data_Criacao'])
            
            # Estatísticas de usuários
            col1, col2, col3 = st.columns(3)
            col1.metric("Total de Usuários", len(usuarios))
            
            admin_count = len(df_usuarios[df_usuarios['Role'] == 'Administrador'])
            tecnico_count = len(df_usuarios[df_usuarios['Role'] == 'Técnico'])
            assistente_count = len(df_usuarios[df_usuarios['Role'] == 'Assistente Administrativo'])
            
            col2.metric("Administradores", admin_count)
            col3.metric("Técnicos/Assistentes", tecnico_count + assistente_count)
            
            # Gráfico de distribuição por role
            role_count = df_usuarios['Role'].value_counts()
            fig_roles = px.pie(
                values=role_count.values,
                names=role_count.index,
                title='Distribuição de Usuários por Função'
            )
            st.plotly_chart(fig_roles, use_container_width=True)
            
            # Tabela de usuários
            st.markdown("### 📋 Lista de Usuários")
            st.dataframe(df_usuarios, use_container_width=True)
            
        else:
            st.info("ℹ️ Nenhum usuário cadastrado no sistema")
            
    except Exception as e:
        st.error(f"❌ Erro ao carregar relatório de usuários: {e}")

# --- FUNÇÕES DE INTERFACE STREAMLIT ---

def sanitizar_nome_arquivo(nome):
    """Remove caracteres inválidos para nomes de arquivo."""
    if not nome:
        return "arquivo"
    # Remove caracteres inválidos e substitui espaços por underscore
    nome_seguro = re.sub(r'[<>:"/\\|?*]', '', nome)
    nome_seguro = nome_seguro.replace(' ', '_')
    # Limita o tamanho do nome para evitar problemas com paths longos
    return nome_seguro[:100]

def generate_csv_zip(df_completo, num_nibs_por_folha, criterio_tipo, criterio_valor):
    """Gera um arquivo ZIP contendo múltiplas folhas CSV com apenas as 10 primeiras colunas."""
    
    max_folha = df_completo['FOLHA'].max()
    
    # Define as 10 primeiras colunas que serão exportadas
    colunas_exportar = [
        'cil', 'prod', 'contador', 'leitura', 'mat_contador',
        'med_fat', 'qtd', 'valor', 'situacao', 'acordo'
    ]
    
    # Verifica se todas as colunas existem no DataFrame
    colunas_disponiveis = [col for col in colunas_exportar if col in df_completo.columns]
    
    if len(colunas_disponiveis) < len(colunas_exportar):
        st.warning(f"⚠️ Algumas colunas não encontradas. Exportando {len(colunas_disponiveis)} colunas.")
    
    # Sanitiza o nome do critério
    criterio_nome_seguro = sanitizar_nome_arquivo(criterio_valor)
    
    # Cria um buffer de memória para o ZIP
    zip_buffer = BytesIO()
    
    with ZipFile(zip_buffer, 'w') as zip_file:
        for i in range(1, max_folha + 1):
            folha_df = df_completo[df_completo['FOLHA'] == i]
            
            # Seleciona apenas as colunas desejadas
            folha_df_export = folha_df[colunas_disponiveis].copy()
            
            # Cria um buffer de memória para o arquivo CSV
            csv_buffer = BytesIO()
            
            # Exporta para CSV
            folha_df_export.to_csv(csv_buffer, index=False, encoding='utf-8-sig', sep=';')
            csv_buffer.seek(0)
            
            # Nome do arquivo personalizado com o critério
            nome_arquivo = f'{criterio_tipo}_{criterio_nome_seguro}_Folha_{i}.csv'
            
            # Adiciona o arquivo CSV ao ZIP
            zip_file.writestr(nome_arquivo, csv_buffer.getvalue())

    zip_buffer.seek(0)
    return zip_buffer.read()

def extrair_cils_do_xlsx(arquivo_xlsx):
    """Extrai a lista de CILs de um arquivo XLSX com diferentes formatos."""
    try:
        # Lê o arquivo XLSX
        df = pd.read_excel(arquivo_xlsx)
        
        st.info(f"📁 Arquivo processado: {len(df)} linhas, {len(df.columns)} colunas")
        
        # Tenta encontrar a coluna com CILs
        coluna_cil = None
        
        # Procura por colunas que podem conter CILs
        possiveis_colunas = ['cil', 'CIL', 'Cil', 'CODIGO', 'código', 'Código', 'numero', 'número']
        
        for col in df.columns:
            col_clean = str(col).strip().lower()
            if any(possivel in col_clean for possivel in ['cil', 'código', 'codigo', 'numero', 'número']):
                coluna_cil = col
                break
        
        # Se não encontrou coluna específica, usa a primeira coluna
        if coluna_cil is None:
            coluna_cil = df.columns[0]
            st.warning(f"ℹ️ Coluna 'cil' não encontrada. Usando a primeira coluna: '{coluna_cil}'")
        else:
            st.success(f"✅ Coluna identificada: '{coluna_cil}'")
        
        # Extrai os CILs
        cils = df[coluna_cil].dropna().astype(str).str.strip()
        
        # Remove possíveis valores de cabeçalho
        cils = cils[~cils.str.lower().isin(['cil', 'cils', 'código', 'codigo', 'nome', 'numero', 'número', ''])]
        
        # Converte para lista e remove duplicatas
        cils_unicos = list(set(cils.tolist()))
        
        # Filtra apenas valores não vazios e válidos
        cils_validos = [cil for cil in cils_unicos if cil and cil != 'nan' and cil.strip()]
        
        st.success(f"📊 {len(cils_validos)} CIL(s) único(s) extraído(s)")
        logger.info(f"CILs extraídos do XLSX: {len(cils_validos)} válidos")
        
        return cils_validos
        
    except Exception as e:
        error_msg = f"❌ Erro ao ler arquivo XLSX: {str(e)}"
        st.error(error_msg)
        logger.error(error_msg)
        return []

def reset_state_form(db_manager, reset_key):
    """Formulário para resetar o estado 'prog'."""
    st.markdown("### 🔄 Resetar Estado de Registros")
    
    tipos_reset = ["PT", "LOCALIDADE", "AVULSO"]
    tipo_reset = st.selectbox("Selecione o Tipo de Reset:", tipos_reset, key=f"reset_type_{reset_key}")
    
    valor_reset = ""
    if tipo_reset in ["PT", "LOCALIDADE"]:
        coluna = tipo_reset
        valores_unicos = db_manager.obter_valores_unicos(coluna)
        if valores_unicos:
            valores_unicos.insert(0, "Selecione...")
            valor_reset = st.selectbox(f"Selecione o valor de **{coluna}** a resetar:", valores_unicos, key=f"reset_value_{reset_key}")
        else:
            st.warning(f"Nenhum valor encontrado para {coluna}")
            
    elif tipo_reset == "AVULSO":
        st.warning("⚠️ O reset 'Avulso' apagará o estado 'prog' de **TODOS** os registros no banco, independentemente de PT/Localidade.")
        
    if st.button(f"🔴 Confirmar Reset - {tipo_reset}", key=f"reset_button_{reset_key}", type="primary"):
        if tipo_reset in ["PT", "LOCALIDADE"] and valor_reset in ["Selecione...", ""]:
            st.error("Por favor, selecione um valor válido para PT ou Localidade.")
        else:
            with st.spinner("Resetando estado..."):
                sucesso, resultado = db_manager.resetar_estado(tipo_reset, valor_reset)
            if sucesso:
                st.success(f"✅ Reset concluído. {resultado} registro(s) tiveram o estado 'prog' removido.")
            else:
                st.error(f"❌ Falha ao resetar: {resultado}")

# --- PÁGINAS DO APLICATIVO ---

def login_page(db_manager):
    """Página de login."""
    st.title("Sistema de Gestão de Dados - Login")
    
    with st.form("login_form"):
        username = st.text_input("Nome de Usuário", placeholder="Digite seu nome de usuário")
        password = st.text_input("Senha", type="password", placeholder="Digite sua senha")
        submitted = st.form_submit_button("Entrar", type="primary")

        if submitted:
            if not username or not password:
                st.error("Por favor, preencha todos os campos.")
                logger.warning("Tentativa de login com campos vazios")
            else:
                with st.spinner("Autenticando..."):
                    user_info = db_manager.autenticar_usuario(username, password)
                if user_info:
                    st.session_state['authenticated'] = True
                    st.session_state['user'] = user_info
                    st.success(f"Bem-vindo(a), {user_info['nome']}!")
                    logger.info(f"Login bem-sucedido para: {username}")
                    st.rerun()
                else:
                    st.error("Nome de usuário ou senha inválidos.")
                    logger.warning(f"Tentativa de login inválida para: {username}")

def manager_page(db_manager):
    """Página principal após o login."""
    
    user = st.session_state['user']
    st.sidebar.markdown(f"**👤 Usuário:** {user['nome']}")
    st.sidebar.markdown(f"**🎯 Função:** {user['role']}")
    
    # Botão de Logout
    if st.sidebar.button("🚪 Sair", use_container_width=True):
        st.session_state['authenticated'] = False
        st.session_state['user'] = None
        logger.info(f"Logout realizado por: {user['nome']}")
        st.rerun()

    # --- Alteração de Senha Pessoal ---
    st.sidebar.markdown("---")
    with st.sidebar.expander("🔐 Alterar Minha Senha"):
        with st.form("alterar_minha_senha"):
            nova_senha = st.text_input("Nova Senha", type="password", key="nova_senha_pessoal")
            confirmar_senha = st.text_input("Confirmar Nova Senha", type="password", key="confirmar_senha_pessoal")
            if st.form_submit_button("Alterar Minha Senha", use_container_width=True):
                if nova_senha and confirmar_senha:
                    if nova_senha == confirmar_senha:
                        if len(nova_senha) >= 6:
                            sucesso, mensagem = db_manager.alterar_senha(user['id'], nova_senha)
                            if sucesso:
                                st.success("✅ Senha alterada com sucesso!")
                            else:
                                st.error(f"❌ {mensagem}")
                        else:
                            st.error("❌ A senha deve ter pelo menos 6 caracteres.")
                    else:
                        st.error("❌ As senhas não coincidem.")
                else:
                    st.error("❌ Preencha todos os campos.")

    st.title(f"Bem-vindo(a), {user['nome']}!")
    
    # --- Controle de Acesso Baseado em Role ---
    if user['role'] == 'Administrador':
        st.header("Gerenciamento de Dados e Relatórios")
        
        # NOVAS ABAS PARA ADMINISTRADOR
        tabs = [
            "Dashboard Geral", 
            "Relatório Operacional", 
            "Análise de Eficiência", 
            "Relatório de Usuários",
            "Importação", 
            "Geração de Folhas", 
            "Gerenciamento de Usuários", 
            "Reset de Estado"
        ]
        selected_tab = st.selectbox("Selecione a Ação:", tabs)
        
    elif user['role'] == 'Assistente Administrativo':
        st.header("Geração de Folhas de Trabalho")
        selected_tab = "Geração de Folhas"
        
    elif user['role'] == 'Técnico':
        st.header("Geração de Folhas de Trabalho")
        selected_tab = "Geração de Folhas"
        
    else:
        st.error("❌ Role de usuário não reconhecido.")
        return

    # =========================================================================
    # NOVAS ABAS DE RELATÓRIOS (APENAS ADMINISTRADOR)
    # =========================================================================
    
    if selected_tab == "Dashboard Geral":
        if user['role'] != 'Administrador':
            st.error("❌ Acesso negado. Apenas Administradores podem acessar o dashboard.")
            return
        mostrar_dashboard_geral(db_manager)
        
    elif selected_tab == "Relatório Operacional":
        if user['role'] != 'Administrador':
            st.error("❌ Acesso negado. Apenas Administradores podem acessar relatórios.")
            return
        mostrar_relatorio_operacional(db_manager)
        
    elif selected_tab == "Análise de Eficiência":
        if user['role'] != 'Administrador':
            st.error("❌ Acesso negado. Apenas Administradores podem acessar análises.")
            return
        mostrar_analise_eficiencia(db_manager)
        
    elif selected_tab == "Relatório de Usuários":
        if user['role'] != 'Administrador':
            st.error("❌ Acesso negado. Apenas Administradores podem acessar relatórios de usuários.")
            return
        mostrar_relatorio_usuarios(db_manager)
        
    # =========================================================================
    # ABAS ORIGINAIS (MANTIDAS)
    # =========================================================================
    
    elif selected_tab == "Importação":
        if user['role'] != 'Administrador':
            st.error("❌ Acesso negado. Apenas Administradores podem importar dados.")
            return
            
        st.markdown("### 📥 Importação de Arquivo CSV (Tabela BD)")
        st.warning("⚠️ Atenção: A importação **substituirá** todos os dados existentes na tabela BD, exceto os registros que já estavam com o estado 'prog'.")

        uploaded_file = st.file_uploader("Selecione o arquivo CSV:", type=["csv"], key="import_csv")

        if uploaded_file is not None:
            if st.button("Processar e Importar para o Banco de Dados", type="primary"):
                with st.spinner("Processando e importando..."):
                    if db_manager.importar_csv(uploaded_file, 'BD'):
                        st.success("🎉 Importação concluída com sucesso!")
                        st.info("O banco de dados foi atualizado.")
                    else:
                        st.error("Falha na importação. Verifique o formato do arquivo e o console para detalhes.")
                        
    elif selected_tab == "Geração de Folhas":
        st.markdown("### 📝 Geração de Folhas de Trabalho")

        tipos_folha = ["PT", "LOCALIDADE", "AVULSO"]
        tipo_selecionado = st.radio("Tipo de Geração:", tipos_folha, horizontal=True)
        
        valor_selecionado = None
        arquivo_xlsx = None
        
        if tipo_selecionado in ["PT", "LOCALIDADE"]:
            coluna = tipo_selecionado
            valores_unicos = db_manager.obter_valores_unicos(coluna)
            if valores_unicos:
                valores_unicos.insert(0, "Selecione...")
                valor_selecionado = st.selectbox(f"Selecione o valor de **{coluna}**:", valores_unicos)
                if valor_selecionado == "Selecione...":
                    valor_selecionado = None
            else:
                st.warning(f"Nenhum valor encontrado para {coluna}")
                
        elif tipo_selecionado == "AVULSO":
            st.markdown("""
            #### 📋 Importar Lista de CILs via Arquivo XLSX
            
            **Instruções:**
            1. Prepare um arquivo Excel (.xlsx) com uma coluna contendo os CILs
            2. A coluna preferencialmente deve se chamar **'cil'**
            3. Faça o upload do arquivo abaixo
            4. O sistema irá automaticamente detectar e extrair os CILs
            """)
            
            arquivo_xlsx = st.file_uploader(
                "Faça upload do arquivo XLSX com a lista de CILs", 
                type=["xlsx"], 
                key="upload_cils_xlsx",
                help="O arquivo deve conter uma coluna com os CILs (preferencialmente chamada 'cil')"
            )
            
            if arquivo_xlsx is not None:
                try:
                    df_preview = pd.read_excel(arquivo_xlsx)
                    st.success(f"✅ Arquivo carregado com sucesso! {len(df_preview)} linhas encontradas.")
                    
                    with st.expander("👀 Visualizar primeiras linhas do arquivo"):
                        st.dataframe(df_preview.head(10))
                        
                    cils_do_arquivo = extrair_cils_do_xlsx(arquivo_xlsx)
                    if cils_do_arquivo:
                        st.info(f"📊 {len(cils_do_arquivo)} CIL(s) único(s) identificado(s)")
                        st.write("**Primeiros CILs encontrados:**", ", ".join(cils_do_arquivo[:5]) + ("..." if len(cils_do_arquivo) > 5 else ""))
                except Exception as e:
                    st.error(f"❌ Erro ao processar arquivo: {e}")

        # --- Seleção de Critério ---
        st.markdown("### 🔍 Critério de Seleção")

        criterio_opcoes = ["Criterio", "Anomalia", "DESC_TP_CLI", "EST_CTR", "sit_div", "desv", "est_inspec"]
        criterio_selecionado = st.radio(
            "Selecione o tipo de critério:",
            criterio_opcoes,
            horizontal=True,
            key="criterio_tipo"
        )

        # Obter valores únicos baseados no critério selecionado
        if criterio_selecionado:
            valores_criterio = db_manager.obter_valores_unicos(criterio_selecionado.lower())
            
            if criterio_selecionado == "Criterio":
                if "SUSP" in valores_criterio:
                    valor_criterio_selecionado = "SUSP"
                    st.info(f"🔍 **Critério selecionado:** {criterio_selecionado} = '{valor_criterio_selecionado}'")
                else:
                    st.error("❌ Critério 'SUSP' não encontrado no banco de dados.")
                    valor_criterio_selecionado = None
            else:
                if valores_criterio:
                    valores_criterio.insert(0, "Selecione...")
                    valor_criterio_selecionado = st.selectbox(
                        f"Selecione o valor para **{criterio_selecionado}**:",
                        valores_criterio,
                        key="criterio_valor"
                    )
                    if valor_criterio_selecionado == "Selecione...":
                        valor_criterio_selecionado = None
                else:
                    st.warning(f"ℹ️ Nenhum valor encontrado para {criterio_selecionado}.")
                    valor_criterio_selecionado = None
        else:
            valor_criterio_selecionado = None
                
        # Parâmetros de Geração
        col1, col2 = st.columns(2)
        with col1:
            num_nibs_por_folha = st.number_input("NIBs por Folha:", min_value=1, value=50)
        with col2:
            max_folhas = st.number_input("Máximo de Folhas a Gerar:", min_value=1, value=10)

        if st.button("Gerar e Baixar Folhas de Trabalho", type="primary"):
            if tipo_selecionado != "AVULSO" and not valor_selecionado:
                st.error("Por favor, selecione um valor válido de PT ou Localidade.")
            elif tipo_selecionado == "AVULSO" and not arquivo_xlsx:
                st.error("Por favor, faça upload de um arquivo XLSX com a lista de CILs.")
            elif not criterio_selecionado or not valor_criterio_selecionado:
                st.error("Por favor, selecione um critério de filtro válido.")
            else:
                cils_validos = None
                if tipo_selecionado == "AVULSO":
                    cils_validos = extrair_cils_do_xlsx(arquivo_xlsx)
                    if not cils_validos:
                        st.error("Nenhum CIL válido encontrado no arquivo XLSX. Verifique o formato do arquivo.")
                        return

                with st.spinner("Gerando folhas de trabalho e atualizando estado no banco..."):
                    df_folhas, cils_nao_encontrados = db_manager.gerar_folhas_trabalho(
                        tipo_selecionado, 
                        valor_selecionado, 
                        max_folhas, 
                        num_nibs_por_folha, 
                        cils_validos,
                        criterio_selecionado,
                        valor_criterio_selecionado
                    )
                    
                if df_folhas is not None and not df_folhas.empty:
                    st.success(f"✅ {df_folhas['FOLHA'].max()} Folhas geradas com sucesso.")
                    
                    colunas_exportadas = ['cil', 'prod', 'contador', 'leitura', 'mat_contador', 
                                        'med_fat', 'qtd', 'valor', 'situacao', 'acordo']
                    st.info(f"📋 Cada folha CSV contém as {len(colunas_exportadas)} primeiras colunas: {', '.join(colunas_exportadas)}")
                    
                    zip_data = generate_csv_zip(df_folhas, num_nibs_por_folha, criterio_selecionado, valor_criterio_selecionado)
                    
                    nome_zip = f"Folhas_{criterio_selecionado}_{sanitizar_nome_arquivo(valor_criterio_selecionado)}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
                    
                    st.download_button(
                        label="📦 Baixar Arquivo ZIP com Folhas (CSV)",
                        data=zip_data,
                        file_name=nome_zip,
                        mime="application/zip",
                        type="primary"
                    )
                    
                    st.info(f"📝 **Nome das folhas:** Cada folha será nomeada como `{criterio_selecionado}_{sanitizar_nome_arquivo(valor_criterio_selecionado)}_Folha_X.csv`")
                    
                    if tipo_selecionado == "AVULSO":
                        if cils_nao_encontrados:
                            st.warning(f"⚠️ {len(cils_nao_encontrados)} CIL(s) não foram encontrados (ou já estavam em 'prog'/não atendem ao critério):")
                            st.code(", ".join(cils_nao_encontrados[:20]) + ("..." if len(cils_nao_encontrados) > 20 else ""))
                            
                        if cils_validos:
                            cils_encontrados = len(cils_validos) - len(cils_nao_encontrados)
                            st.success(f"📊 **Resultado:** {cils_encontrados} de {len(cils_validos)} CIL(s) processados com sucesso.")
                    else:
                        st.success(f"📊 Folhas geradas para {tipo_selecionado}: {valor_selecionado}")
                        
                elif df_folhas is None:
                    if tipo_selecionado == "AVULSO":
                        st.warning("⚠️ Nenhuma folha gerada. Verifique se os CILs existem no banco e atendem ao critério selecionado.")
                    else:
                        st.warning("⚠️ Nenhuma folha gerada. Verifique se existem registros que atendam ao critério selecionado para o valor escolhido.")

    elif selected_tab == "Gerenciamento de Usuários":
        if user['role'] != 'Administrador':
            st.error("❌ Acesso negado. Apenas Administradores podem gerenciar usuários.")
            return

        st.markdown("### 🧑‍💻 Gerenciamento de Usuários")
        
        # --- Criar Novo Usuário ---
        with st.expander("➕ Criar Novo Usuário"):
            with st.form("new_user_form"):
                new_username = st.text_input("Nome de Usuário (login)")
                new_name = st.text_input("Nome Completo")
                new_password = st.text_input("Senha", type="password")
                new_role = st.selectbox("Função:", ['Administrador', 'Assistente Administrativo', 'Técnico'])
                
                if st.form_submit_button("Criar Usuário", type="primary"):
                    if not new_username or not new_password or not new_name:
                        st.error("Preencha todos os campos obrigatórios.")
                    elif len(new_password) < 6:
                        st.error("A senha deve ter pelo menos 6 caracteres.")
                    else:
                        sucesso, mensagem = db_manager.criar_usuario(new_username, new_password, new_name, new_role)
                        if sucesso:
                            st.success(mensagem)
                            st.rerun()
                        else:
                            st.error(mensagem)
                        
        st.markdown("---")
        
        # --- Visualizar/Editar/Excluir Usuários ---
        st.subheader("Lista de Usuários Existentes")
        usuarios = db_manager.obter_usuarios()
        
        if usuarios:
            # Paginação
            items_per_page = 10
            total_pages = (len(usuarios) + items_per_page - 1) // items_per_page
            page_number = st.number_input('Página', min_value=1, max_value=total_pages, value=1, step=1)
            start_index = (page_number - 1) * items_per_page
            end_index = start_index + items_per_page
            usuarios_page = usuarios[start_index:end_index]

            for u in usuarios_page:
                col_u1, col_u2, col_u3, col_u4 = st.columns([2, 2, 2, 3])
                
                user_id = u[0]
                
                with col_u1:
                    st.text_input("Login", u[1], key=f"user_login_{user_id}", disabled=True)
                with col_u2:
                    nome_edit = st.text_input("Nome", u[2], key=f"user_name_{user_id}")
                with col_u3:
                    roles = ['Administrador', 'Assistente Administrativo', 'Técnico']
                    try:
                        current_index = roles.index(u[3])
                    except ValueError:
                        current_index = 0
                    role_edit = st.selectbox("Função", roles, index=current_index, key=f"user_role_{user_id}")

                with col_u4:
                    action = st.radio(
                        "Ação", 
                        ['Nenhuma', 'Editar', 'Alterar Senha', 'Excluir'], 
                        key=f"user_action_{user_id}", 
                        horizontal=True
                    )
                    
                    # Ações
                    if action == 'Editar' and st.button("Salvar Edição", key=f"save_edit_{user_id}"):
                        sucesso, mensagem = db_manager.editar_usuario(user_id, nome_edit, role_edit)
                        if sucesso: 
                            st.success(mensagem)
                            st.rerun()
                        else: 
                            st.error(mensagem)
                        
                    elif action == 'Alterar Senha':
                        new_pass_edit = st.text_input("Nova Senha", type="password", key=f"new_pass_{user_id}")
                        if st.button("Confirmar Alteração de Senha", key=f"save_pass_{user_id}"):
                            if new_pass_edit:
                                if len(new_pass_edit) < 6:
                                    st.error("A senha deve ter pelo menos 6 caracteres.")
                                else:
                                    sucesso, mensagem = db_manager.alterar_senha(user_id, new_pass_edit)
                                    if sucesso: 
                                        st.success(mensagem)
                                        st.rerun()
                                    else: 
                                        st.error(mensagem)
                            else:
                                st.warning("A senha não pode ser vazia.")
                                
                    elif action == 'Excluir' and st.button("⚠️ Confirmar Exclusão", key=f"confirm_delete_{user_id}"):
                        if user_id == 1 and u[1] == 'Admin':
                            st.error("Não é permitido excluir o usuário Administrador Principal padrão.")
                        else:
                            sucesso, mensagem = db_manager.excluir_usuario(user_id)
                            if sucesso: 
                                st.success(mensagem)
                                st.rerun()
                            else: 
                                st.error(mensagem)

            st.write(f"Página {page_number} de {total_pages} - Total de {len(usuarios)} usuários")
        else:
            st.info("Nenhum usuário encontrado no banco de dados.")

    elif selected_tab == "Reset de Estado":
        if user['role'] != 'Administrador':
            st.error("❌ Acesso negado. Apenas Administradores podem resetar o estado.")
            return

        reset_state_form(db_manager, "main")

# --- FUNÇÃO PRINCIPAL ---
def main():
    """Função principal do aplicativo Streamlit."""
    st.set_page_config(
        page_title="Sistema de Gestão de Dados - V.Ferreira",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Inicialização do Estado de Sessão
    if 'authenticated' not in st.session_state:
        st.session_state['authenticated'] = False
        st.session_state['user'] = None

    # Configuração do DB
    try:
        db_manager = PostgresDatabaseManager(POSTGRES_URL)
    except Exception as e:
        st.error("O aplicativo não pôde se conectar ao banco de dados. Verifique as credenciais ou a URL.")
        logger.error(f"Falha na inicialização do banco de dados: {e}")
        return

    # Roteamento
    if st.session_state['authenticated']:
        manager_page(db_manager)
    else:
        login_page(db_manager)

if __name__ == '__main__':
    main()