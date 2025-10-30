# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import datetime
import io
import chardet
import bcrypt
import os
from io import BytesIO
from zipfile import ZipFile

# Para PostgreSQL e ORM (SQLAlchemy)
try:
    from sqlalchemy import create_engine, text, inspect
    from sqlalchemy.exc import SQLAlchemyError
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    st.error("❌ SQLAlchemy não está instalado. Instale com: pip install sqlalchemy psycopg2-binary")
    st.stop()

# --- Configuração das Credenciais do Banco de Dados ---

try:
    POSTGRES_CONFIG = {
        'host': st.secrets["postgres"]["host"],
        'port': st.secrets["postgres"]["port"],
        'database': st.secrets["postgres"]["database"],
        'user': st.secrets["postgres"]["user"],
        'password': st.secrets["postgres"]["password"]
    }
    POSTGRES_URL = (
        f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@"
        f"{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
    )
except KeyError as e:
    st.error(f"❌ Credencial não encontrada nos secrets do Streamlit: {e}")
    st.info("💡 Verifique se todas as credenciais estão configuradas no painel do Streamlit Cloud")
    st.stop()
except Exception as e:
    st.error(f"❌ Erro ao carregar configurações do banco: {e}")
    st.stop()

# --- DatabaseManager para PostgreSQL ---
class PostgresDatabaseManager:
    """Gerencia a conexão e operações com o banco de dados PostgreSQL, 
    incluindo autenticação segura (bcrypt) e operações de dados otimizadas.
    """
    def __init__(self, database_url):
        self.database_url = database_url
        self.engine = None
        
        try:
            self.engine = create_engine(
                self.database_url,
                pool_pre_ping=True,
                connect_args={"connect_timeout": 10}
            )
            self.init_db()
        except Exception as e:
            st.error(f"❌ Erro ao conectar com PostgreSQL: {e}")
            raise

    def _get_conn(self):
        """Retorna uma conexão ativa com o banco."""
        return self.engine.connect()

    # --- Inicialização e Estrutura do BD ---
    def init_db(self):
        """Cria as tabelas 'bd' e 'usuarios' e insere usuários padrão se necessário."""
        try:
            with self.engine.connect() as conn:
                # Tabela BD
                conn.execute(text('''
                    CREATE TABLE IF NOT EXISTS bd (
                        cil TEXT, prod TEXT, contador TEXT, leitura TEXT, mat_contador TEXT,
                        med_fat TEXT, qtd DOUBLE PRECISION, valor DOUBLE PRECISION, situacao TEXT, acordo TEXT,
                        nib TEXT, seq TEXT, localidade TEXT, pt TEXT, desv TEXT,
                        mat_leitura TEXT, desc_uni TEXT, est_contr TEXT, anomalia TEXT, id TEXT,
                        produto TEXT, nome TEXT, criterio TEXT, desc_tp_cli TEXT, tip TEXT,
                        tarifa TEXT, modelo TEXT, lat DOUBLE PRECISION, long DOUBLE PRECISION, fraud TEXT,
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
                        try:
                            conn.execute(
                                text("INSERT INTO usuarios (username, password_hash, nome, role) VALUES (:username, :password_hash, :nome, :role)"),
                                {"username": user[0], "password_hash": user[1], "nome": user[2], "role": user[3]}
                            )
                        except SQLAlchemyError:
                            continue  # Usuário já existe
                    conn.commit()
        except Exception as e:
            st.error(f"❌ Erro ao inicializar banco de dados: {e}")

    # --- Funções de Hashing e Autenticação (bcrypt) ---
    @staticmethod
    def hash_password(password):
        """Gera um hash seguro da senha usando bcrypt."""
        # O salt é gerado automaticamente pelo bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        return hashed.decode('utf-8')

    def autenticar_usuario(self, username, password):
        """Verifica as credenciais do usuário usando bcrypt."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT id, username, password_hash, nome, role FROM usuarios WHERE username = :username"),
                    {"username": username}
                )
                usuario = result.fetchone()
            
            if usuario:
                try:
                    # user[2] é o password_hash
                    if bcrypt.checkpw(password.encode('utf-8'), usuario[2].encode('utf-8')):
                        return {'id': usuario[0], 'username': usuario[1], 'nome': usuario[3], 'role': usuario[4]}
                except ValueError:
                    # Hash inválido ou corrompido
                    return None 
            return None
        except Exception as e:
            st.error(f"❌ Erro na autenticação: {e}")
            return None

    # --- Funções de Gerenciamento de Usuários ---
    def obter_usuarios(self):
        """Retorna a lista de todos os usuários."""
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql_query(text("SELECT id, username, nome, role, data_criacao FROM usuarios ORDER BY username"), conn)
            return df.to_records(index=False).tolist()
        except Exception as e:
            st.error(f"❌ Erro ao obter usuários: {e}")
            return []

    def criar_usuario(self, username, password, nome, role):
        """Cria um novo usuário."""
        try:
            password_hash = self.hash_password(password)
            with self.engine.connect() as conn:
                conn.execute(
                    text("INSERT INTO usuarios (username, password_hash, nome, role) VALUES (:username, :password_hash, :nome, :role)"),
                    {"username": username, "password_hash": password_hash, "nome": nome, "role": role}
                )
                conn.commit()
            return True, "Usuário criado com sucesso!"
        except SQLAlchemyError as e:
            if 'duplicate key value violates unique constraint' in str(e):
                return False, f"O nome de usuário '{username}' já existe."
            return False, f"Erro ao criar usuário: {e}"

    def editar_usuario(self, user_id, nome, role):
        """Edita nome e função de um usuário existente."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("UPDATE usuarios SET nome = :nome, role = :role WHERE id = :id"),
                    {"nome": nome, "role": role, "id": user_id}
                )
                conn.commit()
            return result.rowcount > 0, "Usuário editado com sucesso!"
        except SQLAlchemyError as e:
            return False, f"Erro ao editar usuário: {e}"

    def excluir_usuario(self, user_id):
        """Exclui um usuário pelo ID."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("DELETE FROM usuarios WHERE id = :id"),
                    {"id": user_id}
                )
                conn.commit()
            return result.rowcount > 0, "Usuário excluído com sucesso!"
        except SQLAlchemyError as e:
            return False, f"Erro ao excluir usuário: {e}"

    def alterar_senha(self, user_id, new_password):
        """Altera a senha de um usuário existente."""
        try:
            password_hash = self.hash_password(new_password)
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("UPDATE usuarios SET password_hash = :hash WHERE id = :id"),
                    {"hash": password_hash, "id": user_id}
                )
                conn.commit()
            return result.rowcount > 0, "Senha alterada com sucesso!"
        except SQLAlchemyError as e:
            return False, f"Erro ao alterar senha: {e}"

    # --- Funções Auxiliares de CSV ---
    def _detectar_encoding(self, arquivo_csv):
        """Detecta o encoding do arquivo."""
        raw_data = arquivo_csv.getvalue()
        result = chardet.detect(raw_data)
        return result['encoding']

    def _detectar_separador(self, arquivo_csv, encoding):
        """Detecta o separador mais provável (',' ou ';')."""
        arquivo_csv.seek(0)
        try:
            # Tenta ler 50 linhas
            amostra = arquivo_csv.read(1024 * 50).decode(encoding, errors='ignore')
            
            # Conta ocorrências de ',' e ';'
            virgula_count = amostra.count(',')
            ponto_virgula_count = amostra.count(';')
            
            # Assume o que tiver maior contagem
            if ponto_virgula_count > virgula_count * 2: # Preferência por ';' se for significativamente maior
                return ';'
            else:
                return ','
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
                # low_memory=False: melhora a performance e previne problemas de dtype em grandes arquivos
                df_novo = pd.read_csv(arquivo_csv, sep=separador, encoding=encoding, 
                                      on_bad_lines='skip', header=None, low_memory=False) 
                
                if len(df_novo.columns) < colunas_esperadas:
                    st.error(f"❌ O arquivo BD deve ter pelo menos {colunas_esperadas} colunas.")
                    return False
                
                # Mapeamento de colunas
                column_mapping = {
                    0: 'cil', 1: 'prod', 2: 'contador', 3: 'leitura', 4: 'mat_contador',
                    5: 'med_fat', 6: 'qtd', 7: 'valor', 8: 'situacao', 9: 'acordo',
                    10: 'nib', 11: 'seq', 12: 'localidade', 13: 'pt', 14: 'desv',
                    15: 'mat_leitura', 16: 'desc_uni', 17: 'est_contr', 18: 'anomalia', 19: 'id',
                    20: 'produto', 21: 'nome', 22: 'criterio', 23: 'desc_tp_cli', 24: 'tip',
                    25: 'tarifa', 26: 'modelo', 27: 'lat', 28: 'long', 29: 'fraud',
                    30: 'estado'
                }
                
                df_novo.rename(columns=column_mapping, inplace=True)
                
                # 2. Tratamento e Limpeza (Crucial para Filtros)
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
                    
                    # Preservar estado 'prog' existente (JOIN para eficiência)
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
                return True
            
        except Exception as e:
            st.error(f"❌ Erro ao importar arquivo para PostgreSQL: {str(e)}")
            return False

    def ordenar_tabela_bd(self):
        """Placeholder: A ordenação física é desabilitada. A ordenação será feita nas QUERIES."""
        st.info("ℹ️ Ordenação da tabela BD física desabilitada para otimização de performance.")
        return True

    def obter_valores_unicos(self, coluna, tabela='bd'):
        """Obtém valores únicos de uma coluna, com limpeza de string no SQL."""
        try:
            with self.engine.connect() as conn:
                coluna_sql = coluna.lower()
                    
                # Limpeza (UPPER/TRIM) no SQL para garantir a consistência
                query = text(f"""
                    SELECT DISTINCT UPPER(TRIM({coluna_sql})) as valor_unico
                    FROM {tabela} 
                    WHERE {coluna_sql} IS NOT NULL 
                    AND TRIM({coluna_sql}) != '' 
                    AND TRIM(UPPER({coluna_sql})) NOT IN ('NONE', 'NULL')
                    ORDER BY valor_unico
                """)
                
                df = pd.read_sql_query(query, conn)
                return df['valor_unico'].tolist()
        except Exception as e:
            st.error(f"❌ Erro ao obter valores únicos para {coluna}: {e}")
            return []

    def gerar_folhas_trabalho(self, tipo_folha, valor_selecionado, quantidade_folhas, quantidade_nibs, cils_validos=None):
        """Gera folhas de trabalho, movendo a filtragem e ordenação complexas para o SQL."""
        try:
            with self.engine.connect() as conn:
                
                cils_restantes_nao_encontrados = []
                
                # 1. Construção da Query de Seleção (Performance)
                select_clause = "SELECT * FROM bd"
                where_conditions = ["UPPER(TRIM(criterio)) = 'SUSP'", "LOWER(TRIM(estado)) != 'prog'"]
                query_params = {}
                
                if tipo_folha == "AVULSO" and cils_validos:
                    # Uso de ANY para passar a lista de CILs
                    where_conditions.append("cil = ANY(:cils)")
                    query_params['cils'] = cils_validos
                elif valor_selecionado:
                    # Limpeza do valor e filtro no SQL (UPPER/TRIM)
                    valor_selecionado_limpo = valor_selecionado.strip().upper()
                    coluna_filtro = 'pt' if tipo_folha == "PT" else 'localidade'
                    where_conditions.append(f"UPPER(TRIM({coluna_filtro})) = :valor_filtro")
                    query_params['valor_filtro'] = valor_selecionado_limpo
                
                # 2. Ordenação dentro da Query para garantir a sequência correta
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
                
                # 3. Geração das Folhas (em memória, com base nos NIBs)
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
                    
                    # 4. Atualização de Estado (em bloco via SQL)
                    update_where_conditions = ["LOWER(TRIM(estado)) != 'prog'"]
                    update_params = {'nibs': nibs_na_folha}
                    
                    if tipo_folha == "PT" or tipo_folha == "LOCALIDADE":
                        coluna_filtro = 'pt' if tipo_folha == "PT" else 'localidade'
                        update_where_conditions.append(f"UPPER(TRIM({coluna_filtro})) = :valor_update")
                        update_params['valor_update'] = valor_selecionado.strip().upper()
                    
                    # Uso de ANY para passar a lista de NIBs com segurança na atualização
                    update_query = text(f"""
                        UPDATE bd SET estado = 'prog' 
                        WHERE nib = ANY(:nibs) AND {' AND '.join(update_where_conditions)}
                    """)
                    
                    result = conn.execute(update_query, update_params)
                    total_registros_atualizados += result.rowcount
            
                conn.commit()
                st.success(f"✅ Estado atualizado para 'prog' em {total_registros_atualizados} registros.")
                
                if folhas:
                    resultado_df = pd.concat(folhas, ignore_index=True)
                    return resultado_df, cils_restantes_nao_encontrados
                else:
                    return None, cils_restantes_nao_encontrados
            
        except Exception as e:
            st.error(f"❌ Erro ao gerar folhas no Postgres: {str(e)}")
            return None, []

    def resetar_estado(self, tipo, valor):
        """Reseta o estado 'prog' para o tipo e valor selecionados, com limpeza no SQL."""
        try:
            with self.engine.connect() as conn:
                valor_sql = valor.strip().upper()
                
                if tipo == 'PT':
                    # Aplica UPPER/TRIM no SQL para bater com o dado limpo
                    query = text("UPDATE bd SET estado = '' WHERE LOWER(TRIM(estado)) = 'prog' AND UPPER(TRIM(pt)) = :valor")
                    params = {"valor": valor_sql}
                elif tipo == 'LOCALIDADE':
                    # Aplica UPPER/TRIM no SQL para bater com o dado limpo
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
                return True, registros_afetados
                
        except Exception as e:
            st.error(f"❌ Erro ao resetar o estado no Postgres: {str(e)}")
            return False, 0

# --- Funções de Interface Streamlit ---

def generate_csv_zip(df_completo, num_nibs_por_folha):
    """Gera um arquivo ZIP contendo múltiplas folhas CSV com apenas as 10 primeiras colunas."""
    try:
        if df_completo.empty or 'FOLHA' not in df_completo.columns:
            st.error("❌ Nenhum dado válido para exportar.")
            return None
            
        # Extrai o número máximo de folhas geradas
        max_folha = df_completo['FOLHA'].max()
        
        # Define as 10 primeiras colunas que serão exportadas (A a J)
        colunas_exportar = [
            'cil', 'prod', 'contador', 'leitura', 'mat_contador',
            'med_fat', 'qtd', 'valor', 'situacao', 'acordo'
        ]
        
        # Verifica se todas as colunas existem no DataFrame
        colunas_disponiveis = [col for col in colunas_exportar if col in df_completo.columns]
        
        if len(colunas_disponiveis) < len(colunas_exportar):
            st.warning(f"⚠️ Algumas colunas não encontradas. Exportando {len(colunas_disponiveis)} colunas.")
        
        # Cria um buffer de memória para o ZIP
        zip_buffer = BytesIO()
        
        with ZipFile(zip_buffer, 'w') as zip_file:
            for i in range(1, max_folha + 1):
                folha_df = df_completo[df_completo['FOLHA'] == i]
                
                if folha_df.empty:
                    continue
                    
                # Seleciona apenas as colunas desejadas
                folha_df_export = folha_df[colunas_disponiveis].copy()
                
                # Cria um buffer de memória para o arquivo CSV
                csv_buffer = BytesIO()
                
                # Exporta para CSV
                folha_df_export.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
                csv_buffer.seek(0)
                
                # Adiciona o arquivo CSV ao ZIP
                zip_file.writestr(f'Folha_Trabalho_{i}.csv', csv_buffer.getvalue())

        zip_buffer.seek(0)
        return zip_buffer.read()
    except Exception as e:
        st.error(f"❌ Erro ao gerar arquivo ZIP: {e}")
        return None

def extrair_cils_do_xlsx(arquivo_xlsx):
    """Extrai a lista de CILs de um arquivo XLSX com diferentes formatos."""
    try:
        if arquivo_xlsx is None:
            return []
            
        # Lê o arquivo XLSX
        df = pd.read_excel(arquivo_xlsx)
        
        if df.empty:
            st.warning("⚠️ O arquivo Excel está vazio.")
            return []
        
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
        valores_indesejados = ['cil', 'cils', 'código', 'codigo', 'nome', 'numero', 'número', 'nan', '']
        cils = cils[~cils.str.lower().isin(valores_indesejados)]
        
        # Converte para lista e remove duplicatas
        cils_unicos = list(set(cils.tolist()))
        
        # Filtra apenas valores não vazios e válidos
        cils_validos = [cil for cil in cils_unicos if cil and cil != 'nan' and cil.strip()]
        
        st.success(f"📊 {len(cils_validos)} CIL(s) único(s) extraído(s)")
        
        return cils_validos
        
    except Exception as e:
        st.error(f"❌ Erro ao ler arquivo XLSX: {str(e)}")
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
            st.warning("Nenhum valor encontrado para esta coluna.")
            valor_reset = None
        
    elif tipo_reset == "AVULSO":
        st.warning("⚠️ O reset 'Avulso' apagará o estado 'prog' de **TODOS** os registros no banco, independentemente de PT/Localidade.")
        
    if st.button(f"🔴 Confirmar Reset - {tipo_reset}", key=f"reset_button_{reset_key}"):
        if tipo_reset in ["PT", "LOCALIDADE"] and (valor_reset in ["Selecione...", ""] or valor_reset is None):
            st.error("Por favor, selecione um valor válido para PT ou Localidade.")
        else:
            with st.spinner("Resetando estado..."):
                sucesso, resultado = db_manager.resetar_estado(tipo_reset, valor_reset)
            if sucesso:
                st.success(f"✅ Reset concluído. {resultado} registro(s) tiveram o estado 'prog' removido.")
            else:
                st.error(f"❌ Falha ao resetar: {resultado}")

def gerenciar_usuarios(db_manager):
    """Gerencia usuários (apenas para admin)."""
    st.markdown("### 👥 Gerenciamento de Usuários")
    
    # --- Criar Novo Usuário ---
    with st.expander("➕ Criar Novo Usuário"):
        with st.form("new_user_form"):
            new_username = st.text_input("Nome de Usuário (login)")
            new_name = st.text_input("Nome Completo")
            new_password = st.text_input("Senha", type="password")
            new_role = st.selectbox("Função:", ['Administrador', 'Assistente Administrativo', 'Técnico'])
            
            if st.form_submit_button("👤 Criar Usuário"):
                if new_username and new_password and new_name:
                    sucesso, mensagem = db_manager.criar_usuario(new_username, new_password, new_name, new_role)
                    if sucesso:
                        st.success(mensagem)
                        st.rerun()
                    else:
                        st.error(mensagem)
                else:
                    st.error("Preencha todos os campos obrigatórios.")
                        
    st.markdown("---")
    
    # --- Visualizar/Editar/Excluir Usuários ---
    st.subheader("📋 Lista de Usuários Existentes")
    usuarios = db_manager.obter_usuarios()
    
    if usuarios:
        for u in usuarios:
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
                if action == 'Editar' and st.button("💾 Salvar", key=f"save_edit_{user_id}"):
                    sucesso, mensagem = db_manager.editar_usuario(user_id, nome_edit, role_edit)
                    if sucesso: 
                        st.success(mensagem)
                        st.rerun()
                    else: 
                        st.error(mensagem)
                    
                elif action == 'Alterar Senha':
                    new_pass_edit = st.text_input("Nova Senha", type="password", key=f"new_pass_{user_id}")
                    if st.button("🔑 Alterar Senha", key=f"save_pass_{user_id}"):
                        if new_pass_edit:
                            sucesso, mensagem = db_manager.alterar_senha(user_id, new_pass_edit)
                            if sucesso: 
                                st.success(mensagem)
                                st.rerun()
                            else: 
                                st.error(mensagem)
                        else:
                            st.warning("A senha não pode ser vazia.")
                            
                elif action == 'Excluir' and st.button("🗑️ Excluir", key=f"confirm_delete_{user_id}"):
                    if user_id == 1 and u[1] == 'Admin':
                        st.error("Não é permitido excluir o usuário Administrador Principal padrão.")
                    else:
                        sucesso, mensagem = db_manager.excluir_usuario(user_id)
                        if sucesso: 
                            st.success(mensagem)
                            st.rerun()
                        else: 
                            st.error(mensagem)

    else:
        st.info("Nenhum usuário encontrado no banco de dados.")

def gerar_folhas_trabalho(db_manager, user):
    """Função compartilhada para geração de folhas."""
    st.markdown("### 📝 Gerar Folhas de Trabalho")

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
            st.warning("Nenhum valor encontrado para esta coluna.")
                
    elif tipo_selecionado == "AVULSO":
        st.markdown("""
        #### 📋 Importar Lista de CILs via Arquivo XLSX
        
        **Instruções:**
        1. Prepare um arquivo Excel (.xlsx) com uma coluna contendo os CILs
        2. A coluna preferencialmente deve se chamar **'cil'**
        3. Faça o upload do arquivo abaixo
        4. O sistema irá automaticamente detectar e extrair os CILs
        """)
        
        # Upload de arquivo XLSX (única opção agora)
        arquivo_xlsx = st.file_uploader(
            "Faça upload do arquivo XLSX com a lista de CILs", 
            type=["xlsx"], 
            key="upload_cils_xlsx",
            help="O arquivo deve conter uma coluna com os CILs (preferencialmente chamada 'cil')"
        )
        
        if arquivo_xlsx is not None:
            # Mostrar preview do arquivo
            try:
                df_preview = pd.read_excel(arquivo_xlsx)
                st.success(f"✅ Arquivo carregado com sucesso! {len(df_preview)} linhas encontradas.")
                
                # Mostrar preview das primeiras linhas
                with st.expander("👀 Visualizar primeiras linhas do arquivo"):
                    st.dataframe(df_preview.head(10))
                    
                # Extrair CILs do arquivo
                cils_do_arquivo = extrair_cils_do_xlsx(arquivo_xlsx)
                if cils_do_arquivo:
                    st.info(f"📊 {len(cils_do_arquivo)} CIL(s) único(s) identificado(s)")
                    # Mostrar alguns CILs como exemplo
                    st.write("**Primeiros CILs encontrados:**", ", ".join(cils_do_arquivo[:5]) + ("..." if len(cils_do_arquivo) > 5 else ""))
            except Exception as e:
                st.error(f"❌ Erro ao processar arquivo: {e}")
    
    # Template para download
    if tipo_selecionado == "AVULSO":
        st.markdown("---")
        with st.expander("📋 Baixar Template de Exemplo"):
            st.markdown("""
            **Template recomendado:**
            - Arquivo XLSX com uma coluna chamada **'cil'**
            - Uma lista de CILs na coluna (um por linha)
            - Cabeçalho na primeira linha
            """)
            
            # Criar template exemplo
            template_data = {
                'cil': [
                    '60237270', '60041040', '60110028', '60035026', '60165161',
                    '60228646', '60154604', '60228647', '60011435'
                ]
            }
            df_template = pd.DataFrame(template_data)
            
            # Criar arquivo template para download
            template_buffer = BytesIO()
            with pd.ExcelWriter(template_buffer, engine='xlsxwriter') as writer:
                df_template.to_excel(writer, sheet_name='CILs', index=False)
            template_buffer.seek(0)
            
            st.download_button(
                label="📥 Baixar Template de Exemplo",
                data=template_buffer.getvalue(),
                file_name="template_cils.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
    # Parâmetros de Geração
    col1, col2 = st.columns(2)
    with col1:
        num_nibs_por_folha = st.number_input("NIBs por Folha:", min_value=1, value=50)
    with col2:
        max_folhas = st.number_input("Máximo de Folhas a Gerar:", min_value=1, value=10)

    if st.button("🚀 Gerar e Baixar Folhas de Trabalho"):
        if tipo_selecionado != "AVULSO" and not valor_selecionado:
            st.error("Por favor, selecione um valor válido de PT ou Localidade.")
        elif tipo_selecionado == "AVULSO" and not arquivo_xlsx:
            st.error("Por favor, faça upload de um arquivo XLSX com a lista de CILs.")
        else:
            cils_validos = None
            if tipo_selecionado == "AVULSO":
                # Apenas via arquivo XLSX
                cils_validos = extrair_cils_do_xlsx(arquivo_xlsx)
                if not cils_validos:
                    st.error("Nenhum CIL válido encontrado no arquivo XLSX. Verifique o formato do arquivo.")
                    return

            with st.spinner("Gerando folhas de trabalho e atualizando estado no banco..."):
                df_folhas, cils_nao_encontrados = db_manager.gerar_folhas_trabalho(
                    tipo_selecionado, valor_selecionado, max_folhas, num_nibs_por_folha, cils_validos
                )
                
            if df_folhas is not None and not df_folhas.empty:
                st.success(f"✅ {df_folhas['FOLHA'].max()} Folhas geradas com sucesso.")
                
                # Informar sobre as colunas exportadas
                colunas_exportadas = ['cil', 'prod', 'contador', 'leitura', 'mat_contador', 
                                    'med_fat', 'qtd', 'valor', 'situacao', 'acordo']
                st.info(f"📋 Cada folha CSV contém as {len(colunas_exportadas)} primeiras colunas: {', '.join(colunas_exportadas)}")
                
                zip_data = generate_csv_zip(df_folhas, num_nibs_por_folha)
                
                if zip_data:
                    st.download_button(
                        label="📦 Baixar Arquivo ZIP com Folhas (CSV)",
                        data=zip_data,
                        file_name=f"Folhas_CSV_{tipo_selecionado}_{valor_selecionado or 'AVULSO'}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                        mime="application/zip"
                    )
                
                # Exibir informações específicas para cada tipo
                if tipo_selecionado == "AVULSO":
                    # Exibir CILs não encontrados (apenas para AVULSO)
                    if cils_nao_encontrados:
                        st.warning(f"⚠️ {len(cils_nao_encontrados)} CIL(s) não foram encontrados (ou já estavam em 'prog'/não-SUSP):")
                        st.code(", ".join(cils_nao_encontrados[:20]) + ("..." if len(cils_nao_encontrados) > 20 else ""))
                        
                    # Estatísticas de sucesso (apenas para AVULSO)
                    if cils_validos:
                        cils_encontrados = len(cils_validos) - len(cils_nao_encontrados)
                        st.success(f"📊 **Resultado:** {cils_encontrados} de {len(cils_validos)} CIL(s) processados com sucesso.")
                else:
                    # Mensagem para PT e LOCALIDADE
                    st.success(f"📊 Folhas geradas para {tipo_selecionado}: {valor_selecionado}")
                    
            elif df_folhas is None:
                if tipo_selecionado == "AVULSO":
                    st.warning("⚠️ Nenhuma folha gerada. Verifique se os CILs existem no banco e têm critério 'SUSP'.")
                else:
                    st.warning("⚠️ Nenhuma folha gerada. Verifique se existem registros com critério 'SUSP' para o valor selecionado.")

# --- Páginas do Aplicativo ---
def login_page(db_manager):
    """Página de login."""
    st.title("🔐 Sistema de Gestão de Dados - Login")
    
    # Adicionar informações sobre usuários padrão
    with st.expander("ℹ️ Informações de Acesso"):
        st.markdown("""
        **Usuários Padrão:**
        - **Admin** / admin123 (Administrador)
        - **AssAdm** / adm123 (Assistente Administrativo)
        """)
    
    with st.form("login_form"):
        username = st.text_input("👤 Nome de Usuário")
        password = st.text_input("🔒 Senha", type="password")
        submitted = st.form_submit_button("🚀 Entrar")

        if submitted:
            if not username or not password:
                st.error("❌ Preencha todos os campos.")
            else:
                with st.spinner("Autenticando..."):
                    user_info = db_manager.autenticar_usuario(username, password)
                    if user_info:
                        st.session_state['authenticated'] = True
                        st.session_state['user'] = user_info
                        st.success("✅ Login realizado com sucesso!")
                        st.rerun()
                    else:
                        st.error("❌ Nome de usuário ou senha inválidos.")

def manager_page(db_manager):
    """Página principal após o login."""
    
    if 'user' not in st.session_state:
        st.error("❌ Sessão inválida. Faça login novamente.")
        st.session_state['authenticated'] = False
        st.rerun()
    
    user = st.session_state['user']
    
    # Sidebar melhorada
    with st.sidebar:
        st.markdown(f"### 👋 Olá, {user['nome']}!")
        st.markdown(f"**Função:** {user['role']}")
        
        if st.button("🚪 Sair", use_container_width=True):
            st.session_state['authenticated'] = False
            st.session_state['user'] = None
            st.rerun()
        
        st.markdown("---")
        
        # Alteração de senha pessoal
        with st.expander("🔐 Alterar Minha Senha"):
            with st.form("alterar_minha_senha"):
                nova_senha = st.text_input("Nova Senha", type="password")
                confirmar_senha = st.text_input("Confirmar Nova Senha", type="password")
                if st.form_submit_button("🔄 Alterar Senha"):
                    if nova_senha and confirmar_senha:
                        if nova_senha == confirmar_senha:
                            sucesso, mensagem = db_manager.alterar_senha(user['id'], nova_senha)
                            if sucesso:
                                st.success("✅ Senha alterada com sucesso!")
                            else:
                                st.error(f"❌ {mensagem}")
                        else:
                            st.error("❌ As senhas não coincidem.")
                    else:
                        st.error("❌ Preencha todos os campos.")

    # Controle de acesso baseado em role
    if user['role'] == 'Administrador':
        st.title("🎯 Painel de Administração")
        tabs = st.tabs(["📥 Importação", "📝 Geração de Folhas", "👥 Gerenciamento de Usuários", "🔄 Reset de Estado"])
        
        with tabs[0]:
            # Importação
            st.markdown("### 📥 Importação de Dados")
            st.warning("⚠️ Atenção: A importação **substituirá** todos os dados existentes na tabela BD, exceto os registros que já estavam com o estado 'prog'.")

            uploaded_file = st.file_uploader("Selecione o arquivo CSV:", type=["csv"], key="import_csv")

            if uploaded_file is not None:
                if st.button("🚀 Processar e Importar para o Banco de Dados"):
                    with st.spinner("Processando e importando..."):
                        if db_manager.importar_csv(uploaded_file, 'BD'):
                            st.success("🎉 Importação concluída com sucesso!")
                            st.info("O banco de dados foi atualizado.")
                        else:
                            st.error("Falha na importação. Verifique o formato do arquivo.")
        
        with tabs[1]:
            # Geração de Folhas (compartilhada)
            gerar_folhas_trabalho(db_manager, user)
            
        with tabs[2]:
            # Gerenciamento de Usuários
            gerenciar_usuarios(db_manager)
            
        with tabs[3]:
            # Reset de Estado
            reset_state_form(db_manager, "admin")
            
    else:  # Assistente Administrativo ou Técnico
        st.title("📋 Geração de Folhas de Trabalho")
        gerar_folhas_trabalho(db_manager, user)

# --- Função Principal ---
def main():
    """Função principal do aplicativo Streamlit."""
    st.set_page_config(
        page_title="V.Ferreira (Perdas)", 
        layout="wide",
        page_icon="📊",
        initial_sidebar_state="expanded"
    )
    
    # Estilos CSS para melhor aparência
    st.markdown("""
    <style>
    .main .block-container {
        padding-top: 2rem;
    }
    .stButton button {
        width: 100%;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # 1. Configuração do DB
    try:
        db_manager = PostgresDatabaseManager(POSTGRES_URL)
    except Exception as e:
        st.error(f"❌ O aplicativo não pôde se conectar ao banco de dados: {e}")
        st.info("""
        **Solução de problemas:**
        1. Verifique se as credenciais estão corretas nos secrets do Streamlit
        2. Confirme se o banco PostgreSQL está acessível
        3. Verifique a conexão de rede
        """)
        return

    # 2. Inicialização do Estado de Sessão
    if 'authenticated' not in st.session_state:
        st.session_state['authenticated'] = False
        st.session_state['user'] = None

    # 3. Roteamento
    if st.session_state['authenticated']:
        manager_page(db_manager)
    else:
        login_page(db_manager)

if __name__ == '__main__':
    main()