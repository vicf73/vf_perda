# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import datetime
import io
import chardet
import bcrypt
import os
import time
from io import BytesIO
from zipfile import ZipFile

# --- CONFIGURAÇÃO INICIAL ROBUSTA ---
st.set_page_config(
    page_title="V.Ferreira (perdas)",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': None,
        'Report a bug': None,
        'About': None
    }
)

# CSS AVANÇADO para estabilidade máxima
st.markdown("""
<style>
    .stApp {
        overflow: hidden;
    }
    .main .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }
    div[data-testid="stVerticalBlock"] {
        gap: 0.5rem;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none !important;}
    [data-testid="manage-app"] {display: none !important;}
    
    /* Prevenir animações problemáticas */
    .element-container {
        transition: none !important;
    }
    .stButton > button {
        transition: none !important;
    }
    
    /* Estabilizar containers */
    .block-container {
        position: relative;
        overflow: visible;
    }
</style>
""", unsafe_allow_html=True)

# Inicialização segura do estado
if 'app_initialized' not in st.session_state:
    st.session_state.app_initialized = True
    st.session_state.last_rerun = time.time()

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
except Exception as e:
    st.error("❌ Erro ao carregar as credenciais do banco de dados. Verifique o arquivo de segredos.")
    st.stop()

# Construção da URL de conexão a partir do dicionário de configuração
POSTGRES_URL = (
    f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@"
    f"{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
)

# --- DatabaseManager para PostgreSQL ---
class PostgresDatabaseManager:
    """Gerencia a conexão e operações com o banco de dados PostgreSQL."""
    
    def __init__(self, database_url):
        self.database_url = database_url
        self.engine = None
        
        try:
            self.engine = create_engine(database_url)
            self.init_db()
        except Exception as e:
            st.error(f"❌ Erro ao conectar com PostgreSQL: {e}")
            raise

    def _get_conn(self):
        """Retorna uma conexão ativa com o banco."""
        return self.engine.connect()

    def init_db(self):
        """Cria as tabelas necessárias."""
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
                    conn.execute(
                        text("INSERT INTO usuarios (username, password_hash, nome, role) VALUES (:username, :password_hash, :nome, :role)"),
                        {"username": user[0], "password_hash": user[1], "nome": user[2], "role": user[3]}
                    )
            conn.commit()

    @staticmethod
    def hash_password(password):
        """Gera um hash seguro da senha usando bcrypt."""
        hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        return hashed.decode('utf-8')

    def autenticar_usuario(self, username, password):
        """Verifica as credenciais do usuário."""
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT id, username, password_hash, nome, role FROM usuarios WHERE username = :username"),
                {"username": username}
            )
            usuario = result.fetchone()
        
        if usuario:
            try:
                if bcrypt.checkpw(password.encode('utf-8'), usuario[2].encode('utf-8')):
                    return {'id': usuario[0], 'username': usuario[1], 'nome': usuario[3], 'role': usuario[4]}
            except ValueError:
                return None 
        return None

    def obter_usuarios(self):
        """Retorna a lista de todos os usuários."""
        with self.engine.connect() as conn:
            df = pd.read_sql_query(text("SELECT id, username, nome, role, data_criacao FROM usuarios ORDER BY username"), conn)
        return df.to_records(index=False).tolist()

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

    def _detectar_encoding(self, arquivo_csv):
        """Detecta o encoding do arquivo."""
        raw_data = arquivo_csv.getvalue()
        result = chardet.detect(raw_data)
        return result['encoding']

    def _detectar_separador(self, arquivo_csv, encoding):
        """Detecta o separador mais provável."""
        arquivo_csv.seek(0)
        try:
            amostra = arquivo_csv.read(1024 * 50).decode(encoding, errors='ignore')
            virgula_count = amostra.count(',')
            ponto_virgula_count = amostra.count(';')
            
            if ponto_virgula_count > virgula_count * 2:
                return ';'
            else:
                return ','
        finally:
            arquivo_csv.seek(0)
    
    def importar_csv(self, arquivo_csv, tabela='BD', colunas_esperadas=31):
        """Importa dados do CSV para a tabela BD do PostgreSQL."""
        try:
            encoding = self._detectar_encoding(arquivo_csv)
            separador = self._detectar_separador(arquivo_csv, encoding)

            if tabela == 'BD':
                df_novo = pd.read_csv(arquivo_csv, sep=separador, encoding=encoding, 
                                      on_bad_lines='skip', header=None, low_memory=False) 
                
                if len(df_novo.columns) < colunas_esperadas:
                    st.error(f"❌ O arquivo BD deve ter pelo menos {colunas_esperadas} colunas.")
                    return False
                
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
                
                for col in ['criterio', 'pt', 'localidade', 'nib', 'cil', 'estado']:
                    if col in df_novo.columns:
                        df_novo[col] = df_novo[col].fillna('').astype(str).str.strip()

                df_novo['criterio'] = df_novo['criterio'].str.upper()
                df_novo['pt'] = df_novo['pt'].str.upper()
                df_novo['localidade'] = df_novo['localidade'].str.upper()
                df_novo['estado'] = df_novo['estado'].str.lower()
                
                df_novo['qtd'] = pd.to_numeric(df_novo['qtd'], errors='coerce').fillna(0)
                df_novo['valor'] = pd.to_numeric(df_novo['valor'], errors='coerce').fillna(0)
                df_novo['lat'] = pd.to_numeric(df_novo['lat'], errors='coerce')
                df_novo['long'] = pd.to_numeric(df_novo['long'], errors='coerce')
                
                with self.engine.connect() as conn:
                    df_novo.to_sql('bd_temp_import', conn, if_exists='replace', index=False)
                    
                    update_query = text("""
                        UPDATE bd_temp_import as new 
                        SET estado = 'prog' 
                        FROM bd as old
                        WHERE new.cil = old.cil AND old.estado = 'prog'
                    """)
                    result = conn.execute(update_query)
                    st.info(f"O estado 'prog' foi preservado para {result.rowcount} registro(s) durante a importação.")
                    
                    conn.execute(text("DROP TABLE IF EXISTS bd CASCADE"))
                    conn.execute(text("ALTER TABLE bd_temp_import RENAME TO bd"))
                    conn.commit()

                return True
            
        except Exception as e:
            st.error(f"❌ Erro ao importar arquivo para PostgreSQL: {str(e)}")
            return False

    def obter_valores_unicos(self, coluna, tabela='bd'):
        """Obtém valores únicos de uma coluna."""
        try:
            with self.engine.connect() as conn:
                coluna_sql = coluna.lower()
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
        """Gera folhas de trabalho."""
        try:
            with self.engine.connect() as conn:
                
                cils_restantes_nao_encontrados = []
                
                select_clause = "SELECT * FROM bd"
                where_conditions = ["UPPER(TRIM(criterio)) = 'SUSP'", "LOWER(TRIM(estado)) != 'prog'"]
                query_params = {}
                
                if tipo_folha == "AVULSO" and cils_validos:
                    where_conditions.append("cil = ANY(:cils)")
                    query_params['cils'] = cils_validos
                elif valor_selecionado:
                    valor_selecionado_limpo = valor_selecionado.strip().upper()
                    coluna_filtro = 'pt' if tipo_folha == "PT" else 'localidade'
                    where_conditions.append(f"UPPER(TRIM({coluna_filtro})) = :valor_filtro")
                    query_params['valor_filtro'] = valor_selecionado_limpo
                
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
                    
                    update_where_conditions = ["LOWER(TRIM(estado)) != 'prog'"]
                    update_params = {'nibs': nibs_na_folha}
                    
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
                
                if folhas:
                    resultado_df = pd.concat(folhas, ignore_index=True)
                    return resultado_df, cils_restantes_nao_encontrados
                else:
                    return None, cils_restantes_nao_encontrados
            
        except Exception as e:
            st.error(f"❌ Erro ao gerar folhas no Postgres: {str(e)}")
            return None, []

    def resetar_estado(self, tipo, valor):
        """Reseta o estado 'prog'."""
        try:
            with self.engine.connect() as conn:
                valor_sql = valor.strip().upper()
                
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
                return True, registros_afetados
                
        except Exception as e:
            st.error(f"❌ Erro ao resetar o estado no Postgres: {str(e)}")
            return False, 0

# --- Funções Auxiliares ---

def generate_csv_zip(df_completo, num_nibs_por_folha):
    """Gera um arquivo ZIP com folhas CSV."""
    
    max_folha = df_completo['FOLHA'].max()
    
    colunas_exportar = [
        'cil', 'prod', 'contador', 'leitura', 'mat_contador',
        'med_fat', 'qtd', 'valor', 'situacao', 'acordo'
    ]
    
    colunas_disponiveis = [col for col in colunas_exportar if col in df_completo.columns]
    
    if len(colunas_disponiveis) < len(colunas_exportar):
        st.warning(f"⚠️ Algumas colunas não encontradas. Exportando {len(colunas_disponiveis)} colunas.")
    
    zip_buffer = BytesIO()
    
    with ZipFile(zip_buffer, 'w') as zip_file:
        for i in range(1, max_folha + 1):
            folha_df = df_completo[df_completo['FOLHA'] == i]
            folha_df_export = folha_df[colunas_disponiveis].copy()
            
            csv_buffer = BytesIO()
            folha_df_export.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            csv_buffer.seek(0)
            
            zip_file.writestr(f'Folha_Trabalho_{i}.csv', csv_buffer.getvalue())

    zip_buffer.seek(0)
    return zip_buffer.read()

def extrair_cils_do_xlsx(arquivo_xlsx):
    """Extrai a lista de CILs de um arquivo XLSX."""
    try:
        df = pd.read_excel(arquivo_xlsx)
        
        st.info(f"📁 Arquivo processado: {len(df)} linhas, {len(df.columns)} colunas")
        
        coluna_cil = None
        possiveis_colunas = ['cil', 'CIL', 'Cil', 'CODIGO', 'código', 'Código', 'numero', 'número']
        
        for col in df.columns:
            col_clean = str(col).strip().lower()
            if any(possivel in col_clean for possivel in ['cil', 'código', 'codigo', 'numero', 'número']):
                coluna_cil = col
                break
        
        if coluna_cil is None:
            coluna_cil = df.columns[0]
            st.warning(f"ℹ️ Coluna 'cil' não encontrada. Usando a primeira coluna: '{coluna_cil}'")
        else:
            st.success(f"✅ Coluna identificada: '{coluna_cil}'")
        
        cils = df[coluna_cil].dropna().astype(str).str.strip()
        cils = cils[~cils.str.lower().isin(['cil', 'cils', 'código', 'codigo', 'nome', 'numero', 'número', ''])]
        
        cils_unicos = list(set(cils.tolist()))
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
        
    elif tipo_reset == "AVULSO":
        st.warning("⚠️ O reset 'Avulso' apagará o estado 'prog' de **TODOS** os registros no banco.")
        
    if st.button(f"🔴 Confirmar Reset - {tipo_reset}", key=f"reset_button_{reset_key}"):
        if tipo_reset in ["PT", "LOCALIDADE"] and valor_reset in ["Selecione...", ""]:
            st.error("Por favor, selecione um valor válido para PT ou Localidade.")
        else:
            sucesso, resultado = db_manager.resetar_estado(tipo_reset, valor_reset)
            if sucesso:
                st.success(f"✅ Reset concluído. {resultado} registro(s) tiveram o estado 'prog' removido.")
            else:
                st.error(f"❌ Falha ao resetar: {resultado}")

# --- Páginas do Aplicativo ---

def login_page(db_manager):
    """Página de login."""
    # Usar container para estabilidade
    with st.container():
        st.title("Sistema de Gestão de Dados - Login")
        
        with st.form("login_form", key="login_form_unique"):
            username = st.text_input("Nome de Usuário", key="username_input_unique")
            password = st.text_input("Senha", type="password", key="password_input_unique")
            submitted = st.form_submit_button("Entrar", key="login_submit_unique")

            if submitted:
                user_info = db_manager.autenticar_usuario(username, password)
                if user_info:
                    st.session_state.authenticated = True
                    st.session_state.user = user_info
                    # Evitar rerun() - usar approach mais suave
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("Nome de usuário ou senha inválidos.")

def safe_rerun():
    """Função segura para rerun com prevenção de erros."""
    current_time = time.time()
    if current_time - st.session_state.get('last_rerun', 0) > 2:  # Mínimo 2 segundos entre reruns
        st.session_state.last_rerun = current_time
        st.rerun()

def manager_page(db_manager):
    """Página principal após o login."""
    
    user = st.session_state.user
    st.sidebar.markdown(f"**Usuário:** {user['nome']} ({user['role']})")
    
    # Botão de Logout com proteção
    if st.sidebar.button("Sair", key="logout_button_unique"):
        st.session_state.authenticated = False
        st.session_state.user = None
        time.sleep(0.5)
        safe_rerun()

    # Alteração de Senha Pessoal
    st.sidebar.markdown("---")
    with st.sidebar.expander("🔐 Alterar Minha Senha", key="alterar_senha_expander"):
        with st.form("alterar_minha_senha_form", key="alterar_senha_form_unique"):
            nova_senha = st.text_input("Nova Senha", type="password", key="nova_senha_pessoal_unique")
            confirmar_senha = st.text_input("Confirmar Nova Senha", type="password", key="confirmar_senha_pessoal_unique")
            if st.form_submit_button("Alterar Minha Senha", key="alterar_senha_submit_unique"):
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

    st.title(f"Bem-vindo, {user['nome']}!")
    
    # Controle de Acesso Baseado em Role
    if user['role'] == 'Administrador':
        st.header("Gerenciamento de Dados e Folhas de Trabalho")
        tabs = ["Importação", "Geração de Folhas", "Gerenciamento de Usuários", "Reset de Estado"]
        selected_tab = st.selectbox("Selecione a Ação:", tabs, key="main_tab_selector_unique")
        
    elif user['role'] in ['Assistente Administrativo', 'Técnico']:
        st.header("Geração de Folhas de Trabalho")
        selected_tab = "Geração de Folhas"
        
    else:
        st.error("❌ Role de usuário não reconhecido.")
        return

    # Container principal para estabilidade
    with st.container():
        # ABA 1: IMPORTAÇÃO DE CSV
        if selected_tab == "Importação":
            if user['role'] != 'Administrador':
                st.error("❌ Acesso negado. Apenas Administradores podem importar dados.")
                return
                
            st.markdown("### 📥 Importação de Arquivo CSV (Tabela BD)")
            st.warning("⚠️ A importação substituirá todos os dados existentes na tabela BD.")

            uploaded_file = st.file_uploader("Selecione o arquivo CSV:", type=["csv"], key="import_csv_unique")

            if uploaded_file is not None:
                if st.button("Processar e Importar para o Banco de Dados", key="import_button_unique"):
                    with st.spinner("Processando e importando..."):
                        if db_manager.importar_csv(uploaded_file, 'BD'):
                            st.success("🎉 Importação concluída com sucesso!")
                        else:
                            st.error("Falha na importação. Verifique o formato do arquivo.")
                            
        # ABA 2: GERAÇÃO DE FOLHAS
        elif selected_tab == "Geração de Folhas":
            st.markdown("### 📝 Geração de Folhas de Trabalho")

            tipos_folha = ["PT", "LOCALIDADE", "AVULSO"]
            tipo_selecionado = st.radio("Tipo de Geração: V.Ferreira", tipos_folha, 
                                      horizontal=True, key="tipo_folha_radio_unique")
            
            valor_selecionado = None
            arquivo_xlsx = None
            
            if tipo_selecionado in ["PT", "LOCALIDADE"]:
                coluna = tipo_selecionado
                valores_unicos = db_manager.obter_valores_unicos(coluna)
                if valores_unicos:
                    valores_unicos.insert(0, "Selecione...")
                    valor_selecionado = st.selectbox(f"Selecione o valor de **{coluna}**:", 
                                                   valores_unicos, key=f"valor_{coluna}_unique")
                    if valor_selecionado == "Selecione...":
                        valor_selecionado = None
                    
            elif tipo_selecionado == "AVULSO":
                st.markdown("#### 📋 Importar Lista de CILs via Arquivo XLSX")
                
                arquivo_xlsx = st.file_uploader(
                    "Faça upload do arquivo XLSX com a lista de CILs", 
                    type=["xlsx"], 
                    key="upload_cils_xlsx_unique"
                )
                
                if arquivo_xlsx is not None:
                    try:
                        df_preview = pd.read_excel(arquivo_xlsx)
                        st.success(f"✅ Arquivo carregado com sucesso! {len(df_preview)} linhas encontradas.")
                        
                        with st.expander("👀 Visualizar primeiras linhas do arquivo", key="preview_expander_unique"):
                            st.dataframe(df_preview.head(10))
                            
                        cils_do_arquivo = extrair_cils_do_xlsx(arquivo_xlsx)
                        if cils_do_arquivo:
                            st.write("**Primeiros CILs encontrados:**", ", ".join(cils_do_arquivo[:5]) + ("..." if len(cils_do_arquivo) > 5 else ""))
                    except Exception as e:
                        st.error(f"❌ Erro ao processar arquivo: {e}")
            
            # Parâmetros de Geração
            col1, col2 = st.columns(2)
            with col1:
                num_nibs_por_folha = st.number_input("NIBs por Folha:", min_value=1, value=50, key="nibs_por_folha_unique")
            with col2:
                max_folhas = st.number_input("Máximo de Folhas a Gerar:", min_value=1, value=10, key="max_folhas_unique")

            if st.button("Gerar e Baixar Folhas de Trabalho", key="gerar_folhas_button_unique"):
                if tipo_selecionado != "AVULSO" and not valor_selecionado:
                    st.error("Por favor, selecione um valor válido de PT ou Localidade.")
                elif tipo_selecionado == "AVULSO" and not arquivo_xlsx:
                    st.error("Por favor, faça upload de um arquivo XLSX com a lista de CILs.")
                else:
                    cils_validos = None
                    if tipo_selecionado == "AVULSO":
                        cils_validos = extrair_cils_do_xlsx(arquivo_xlsx)
                        if not cils_validos:
                            st.error("Nenhum CIL válido encontrado no arquivo XLSX.")
                            return

                    with st.spinner("Gerando folhas de trabalho..."):
                        df_folhas, cils_nao_encontrados = db_manager.gerar_folhas_trabalho(
                            tipo_selecionado, valor_selecionado, max_folhas, num_nibs_por_folha, cils_validos
                        )
                        
                    if df_folhas is not None and not df_folhas.empty:
                        st.success(f"✅ {df_folhas['FOLHA'].max()} Folhas geradas com sucesso.")
                        
                        zip_data = generate_csv_zip(df_folhas, num_nibs_por_folha)
                        
                        st.download_button(
                            label="📦 Baixar Arquivo ZIP com Folhas (CSV)",
                            data=zip_data,
                            file_name=f"Folhas_CSV_{tipo_selecionado}_{valor_selecionado or 'AVULSO'}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                            mime="application/zip",
                            key="download_zip_unique"
                        )
                        
                        if tipo_selecionado == "AVULSO" and cils_nao_encontrados:
                            st.warning(f"⚠️ {len(cils_nao_encontrados)} CIL(s) não foram encontrados:")
                            st.code(", ".join(cils_nao_encontrados[:20]) + ("..." if len(cils_nao_encontrados) > 20 else ""))
                            
                    else:
                        st.warning("⚠️ Nenhuma folha gerada. Verifique os critérios de seleção.")

        # ABA 3: GERENCIAMENTO DE USUÁRIOS
        elif selected_tab == "Gerenciamento de Usuários":
            if user['role'] != 'Administrador':
                st.error("❌ Acesso negado. Apenas Administradores podem gerenciar usuários.")
                return

            st.markdown("### 🧑‍💻 Gerenciamento de Usuários")
            
            with st.expander("➕ Criar Novo Usuário", key="criar_usuario_expander_unique"):
                with st.form("new_user_form", key="new_user_form_unique"):
                    new_username = st.text_input("Nome de Usuário (login)", key="new_username_unique")
                    new_name = st.text_input("Nome Completo", key="new_name_unique")
                    new_password = st.text_input("Senha", type="password", key="new_password_unique")
                    new_role = st.selectbox("Função:", ['Administrador', 'Assistente Administrativo', 'Técnico'], key="new_role_unique")
                    
                    if st.form_submit_button("Criar Usuário", key="create_user_button_unique"):
                        if new_username and new_password:
                            sucesso, mensagem = db_manager.criar_usuario(new_username, new_password, new_name, new_role)
                            if sucesso:
                                st.success(mensagem)
                                time.sleep(1)
                                safe_rerun()
                            else:
                                st.error(mensagem)
                        else:
                            st.error("Preencha todos os campos obrigatórios.")
                            
            st.markdown("---")
            st.subheader("Lista de Usuários Existentes")
            usuarios = db_manager.obter_usuarios()
            
            if usuarios:
                for u in usuarios:
                    col_u1, col_u2, col_u3, col_u4 = st.columns([2, 2, 2, 3])
                    
                    user_id = u[0]
                    
                    with col_u1:
                        st.text_input("Login", u[1], key=f"user_login_{user_id}_unique", disabled=True)
                    with col_u2:
                        nome_edit = st.text_input("Nome", u[2], key=f"user_name_{user_id}_unique")
                    with col_u3:
                        roles = ['Administrador', 'Assistente Administrativo', 'Técnico']
                        try:
                            current_index = roles.index(u[3])
                        except ValueError:
                            current_index = 0
                        role_edit = st.selectbox("Função", roles, index=current_index, key=f"user_role_{user_id}_unique")

                    with col_u4:
                        action = st.radio(
                            "Ação", 
                            ['Nenhuma', 'Editar', 'Alterar Senha', 'Excluir'], 
                            key=f"user_action_{user_id}_unique", 
                            horizontal=True
                        )
                        
                        if action == 'Editar' and st.button("Salvar Edição", key=f"save_edit_{user_id}_unique"):
                            sucesso, mensagem = db_manager.editar_usuario(user_id, nome_edit, role_edit)
                            if sucesso: 
                                st.success(mensagem)
                                time.sleep(1)
                                safe_rerun()
                            else: 
                                st.error(mensagem)
                            
                        elif action == 'Alterar Senha':
                            new_pass_edit = st.text_input("Nova Senha", type="password", key=f"new_pass_{user_id}_unique")
                            if st.button("Confirmar Alteração de Senha", key=f"save_pass_{user_id}_unique"):
                                if new_pass_edit:
                                    sucesso, mensagem = db_manager.alterar_senha(user_id, new_pass_edit)
                                    if sucesso: 
                                        st.success(mensagem)
                                        time.sleep(1)
                                        safe_rerun()
                                    else: 
                                        st.error(mensagem)
                                else:
                                    st.warning("A senha não pode ser vazia.")
                                    
                        elif action == 'Excluir' and st.button("⚠️ Confirmar Exclusão", key=f"confirm_delete_{user_id}_unique"):
                            if user_id == 1 and u[1] == 'Admin':
                                st.error("Não é permitido excluir o usuário Administrador Principal padrão.")
                            else:
                                sucesso, mensagem = db_manager.excluir_usuario(user_id)
                                if sucesso: 
                                    st.success(mensagem)
                                    time.sleep(1)
                                    safe_rerun()
                                else: 
                                    st.error(mensagem)

            else:
                st.info("Nenhum usuário encontrado no banco de dados.")

        # ABA 4: RESET DE ESTADO
        elif selected_tab == "Reset de Estado":
            if user['role'] != 'Administrador':
                st.error("❌ Acesso negado. Apenas Administradores podem resetar o estado.")
                return

            reset_state_form(db_manager, "main_unique")

# --- Função Principal ---
def main():
    """Função principal do aplicativo Streamlit."""
    
    try:
        # Configuração do DB
        try:
            db_manager = PostgresDatabaseManager(POSTGRES_URL)
        except Exception:
            st.error("O aplicativo não pôde se conectar ao banco de dados. Verifique as credenciais ou a URL.")
            return

        # Inicialização do Estado de Sessão
        if 'authenticated' not in st.session_state:
            st.session_state.authenticated = False
            st.session_state.user = None

        # Roteamento
        if st.session_state.authenticated:
            manager_page(db_manager)
        else:
            login_page(db_manager)
            
    except Exception as e:
        st.error("Erro temporário. Por favor, recarregue a página.")
        st.stop()

if __name__ == '__main__':
    main()