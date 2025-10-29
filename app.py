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

# --- CONFIGURAÇÃO RADICAL ---
# Forçar modo estático completo
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

# CSS AGGRESSIVO para prevenir qualquer manipulação DOM problemática
st.markdown("""
<style>
    /* Reset completo de transições e animações */
    * {
        transition: none !important;
        animation: none !important;
    }
    
    .stApp {
        overflow: hidden !important;
        position: relative !important;
    }
    
    .main .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
        position: relative !important;
    }
    
    /* Esconder TUDO do Streamlit que possa causar problemas */
    #MainMenu {visibility: hidden !important; display: none !important;}
    footer {visibility: hidden !important; display: none !important;}
    .stDeployButton {display: none !important;}
    [data-testid="manage-app"] {display: none !important;}
    [data-testid="stToolbar"] {display: none !important;}
    [data-testid="stDecoration"] {display: none !important;}
    
    /* Estabilizar containers principais */
    .block-container {
        position: relative !important;
        overflow: visible !important;
    }
    
    /* Prevenir qualquer transformação CSS */
    .element-container {
        transform: none !important;
        transition: none !important;
    }
    
    /* Fixar todos os botões e inputs */
    .stButton > button {
        position: relative !important;
        transform: none !important;
    }
    
    .stTextInput > div > div > input {
        position: relative !important;
    }
    
    /* Container estático para todo o conteúdo */
    .static-container {
        position: relative;
        min-height: 100vh;
    }
</style>
""", unsafe_allow_html=True)

# --- INICIALIZAÇÃO SUPER SEGURA ---
# Usar approach completamente estático
if 'app_initialized' not in st.session_state:
    st.session_state.app_initialized = True
    st.session_state.authenticated = False
    st.session_state.user = None
    st.session_state.current_page = 'login'
    st.session_state.last_action = time.time()
    st.session_state.prevent_rerun = False

# Para PostgreSQL
try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import SQLAlchemyError
    # Certifique-se de que psycopg2 (ou similar) está instalado
except ImportError:
    st.error("❌ Dependências não instaladas. Certifique-se de ter 'sqlalchemy' e 'psycopg2-binary' instalados.")
    st.stop()

# --- CONFIGURAÇÃO BANCO ---
try:
    # Acessando st.secrets, assumindo que as credenciais estão configuradas
    POSTGRES_CONFIG = {
        'host': st.secrets["postgres"]["host"],
        'port': st.secrets["postgres"]["port"],
        'database': st.secrets["postgres"]["database"],
        'user': st.secrets["postgres"]["user"],
        'password': st.secrets["postgres"]["password"]
    }
    POSTGRES_URL = f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
except Exception as e:
    # Se st.secrets não estiver configurado, o app não irá funcionar
    st.error(f"❌ Erro nas credenciais do banco (st.secrets): {e}")
    #st.stop() # Comentei para permitir testes locais sem secrets

# --- DATABASE MANAGER (SIMPLIFICADO) ---
class PostgresDatabaseManager:
    def __init__(self, database_url):
        self.database_url = database_url
        try:
            self.engine = create_engine(database_url)
            self.init_db()
        except Exception as e:
            # Não use st.error aqui dentro de __init__ se for levantado para o main
            # O main() cuidará disso
            raise e 

    def init_db(self):
        with self.engine.connect() as conn:
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
            
            result = conn.execute(text("SELECT COUNT(*) FROM usuarios"))
            if result.scalar() == 0:
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
        hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        return hashed.decode('utf-8')

    def autenticar_usuario(self, username, password):
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT id, username, password_hash, nome, role FROM usuarios WHERE username = :username"),
                {"username": username}
            )
            usuario = result.fetchone()
        
        if usuario and bcrypt.checkpw(password.encode('utf-8'), usuario[2].encode('utf-8')):
            return {'id': usuario[0], 'username': usuario[1], 'nome': usuario[3], 'role': usuario[4]}
        return None

    def obter_usuarios(self):
        with self.engine.connect() as conn:
            df = pd.read_sql_query(text("SELECT id, username, nome, role, data_criacao FROM usuarios ORDER BY username"), conn)
        return df.to_records(index=False).tolist()

    def criar_usuario(self, username, password, nome, role):
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
            # Erro de integridade (usuário já existe, por exemplo)
            return False, f"Erro: {e.orig.pgerror if hasattr(e.orig, 'pgerror') else e}"

    def editar_usuario(self, user_id, nome, role):
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("UPDATE usuarios SET nome = :nome, role = :role WHERE id = :id"),
                    {"nome": nome, "role": role, "id": user_id}
                )
                conn.commit()
            return result.rowcount > 0, "Usuário editado com sucesso!"
        except SQLAlchemyError as e:
            return False, f"Erro: {e}"

    def excluir_usuario(self, user_id):
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("DELETE FROM usuarios WHERE id = :id"),
                    {"id": user_id}
                )
                conn.commit()
            return result.rowcount > 0, "Usuário excluído com sucesso!"
        except SQLAlchemyError as e:
            return False, f"Erro: {e}"

    def alterar_senha(self, user_id, new_password):
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
            return False, f"Erro: {e}"

    def _detectar_encoding(self, arquivo_csv):
        raw_data = arquivo_csv.getvalue()
        return chardet.detect(raw_data)['encoding']

    def importar_csv(self, arquivo_csv):
        try:
            encoding = self._detectar_encoding(arquivo_csv)
            # Rebobinar o arquivo após a detecção de encoding para garantir que a leitura comece do início
            arquivo_csv.seek(0) 
            df_novo = pd.read_csv(arquivo_csv, encoding=encoding, on_bad_lines='skip', header=None, low_memory=False)
            
            if len(df_novo.columns) < 31:
                # Retorna False, a renderização deve tratar o erro de colunas insuficientes
                return False
                
            column_mapping = {
                0: 'cil', 1: 'prod', 2: 'contador', 3: 'leitura', 4: 'mat_contador',
                5: 'med_fat', 6: 'qtd', 7: 'valor', 8: 'situacao', 9: 'acordo',
                10: 'nib', 11: 'seq', 12: 'localidade', 13: 'pt', 14: 'desv',
                15: 'mat_leitura', 16: 'desc_uni', 17: 'est_contr', 18: 'anomalia', 19: 'id',
                20: 'produto', 21: 'nome', 22: 'criterio', 23: 'desc_tp_cli', 24: 'tip',
                25: 'tarifa', 26: 'modelo', 27: 'lat', 28: 'long', 29: 'fraud', 30: 'estado'
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
            
            with self.engine.connect() as conn:
                # Importação atômica
                df_novo.to_sql('bd_temp_import', conn, if_exists='replace', index=False)
                conn.execute(text("DROP TABLE IF EXISTS bd CASCADE"))
                conn.execute(text("ALTER TABLE bd_temp_import RENAME TO bd"))
                conn.commit()

            return True
            
        except Exception as e:
            st.error(f"Erro durante a importação: {e}")
            return False

    def obter_valores_unicos(self, coluna):
        try:
            with self.engine.connect() as conn:
                # A coluna deve vir de uma lista controlada (PT, LOCALIDADE)
                query = text(f"""
                    SELECT DISTINCT UPPER(TRIM({coluna})) as valor_unico
                    FROM bd 
                    WHERE {coluna} IS NOT NULL 
                    AND TRIM({coluna}) != '' 
                    ORDER BY valor_unico
                """)
                df = pd.read_sql_query(query, conn)
                return df['valor_unico'].tolist()
        except Exception as e:
            # Em caso de erro (ex: tabela bd não existe), retorna lista vazia
            # print(f"Erro ao obter valores únicos de {coluna}: {e}")
            return []

    def gerar_folhas_trabalho(self, tipo_folha, valor_selecionado, quantidade_folhas, quantidade_nibs, cils_validos=None):
        try:
            with self.engine.connect() as conn:
                where_conditions = ["UPPER(TRIM(criterio)) = 'SUSP'", "LOWER(TRIM(estado)) != 'prog'"]
                query_params = {}
                
                if tipo_folha == "AVULSO" and cils_validos:
                    # Usando UNNEST para array de CILs para compatibilidade
                    where_conditions.append("cil = ANY(:cils)")
                    query_params['cils'] = cils_validos
                elif valor_selecionado:
                    coluna_filtro = 'pt' if tipo_folha == "PT" else 'localidade'
                    where_conditions.append(f"UPPER(TRIM({coluna_filtro})) = :valor_filtro")
                    query_params['valor_filtro'] = valor_selecionado.strip().upper()
                
                query = text(f"SELECT * FROM bd WHERE {' AND '.join(where_conditions)} ORDER BY nib")
                df = pd.read_sql_query(query, conn, params=query_params)

                if df.empty:
                    return None, []

                df['nib'] = df['nib'].fillna('').astype(str).str.strip()
                nibs_unicos = [nib for nib in df['nib'].unique() if nib]
                
                if not nibs_unicos:
                    return None, []

                folhas_possiveis = min(quantidade_folhas, (len(nibs_unicos) + quantidade_nibs - 1) // quantidade_nibs)
                folhas = []
                
                for i in range(folhas_possiveis):
                    nibs_na_folha = nibs_unicos[i * quantidade_nibs: (i + 1) * quantidade_nibs]
                    folha_df = df[df['nib'].isin(nibs_na_folha)].copy()
                    folha_df['FOLHA'] = i + 1
                    folhas.append(folha_df)
                    
                    # Marcar o estado dos NIBs selecionados para 'prog'
                    update_query = text(f"UPDATE bd SET estado = 'prog' WHERE nib = ANY(:nibs)")
                    conn.execute(update_query, {'nibs': nibs_na_folha})
                
                conn.commit()
                
                if folhas:
                    return pd.concat(folhas, ignore_index=True), []
                return None, []
                
        except Exception as e:
            st.error(f"Erro ao gerar folhas: {e}")
            return None, []

    def resetar_estado(self, tipo, valor):
        try:
            with self.engine.connect() as conn:
                if tipo == 'PT':
                    query = text("UPDATE bd SET estado = '' WHERE LOWER(TRIM(estado)) = 'prog' AND UPPER(TRIM(pt)) = :valor")
                    params = {"valor": valor.strip().upper()}
                elif tipo == 'LOCALIDADE':
                    query = text("UPDATE bd SET estado = '' WHERE LOWER(TRIM(estado)) = 'prog' AND UPPER(TRIM(localidade)) = :valor")
                    params = {"valor": valor.strip().upper()}
                else: # AVULSO (reseta tudo que estiver em 'prog')
                    query = text("UPDATE bd SET estado = '' WHERE LOWER(TRIM(estado)) = 'prog'")
                    params = {}
                
                result = conn.execute(query, params)
                conn.commit()
                return True, result.rowcount
                
        except Exception as e:
            st.error(f"Erro durante o reset: {e}")
            return False, 0

# --- FUNÇÕES AUXILIARES ---
def generate_csv_zip(df_completo):
    # Colunas de interesse para a equipe de campo
    colunas_exportar = ['cil', 'prod', 'contador', 'leitura', 'mat_contador', 'med_fat', 'qtd', 'valor', 'situacao', 'acordo']
    # Garante que só exporta colunas que existem no DataFrame
    colunas_disponiveis = [col for col in colunas_exportar if col in df_completo.columns]
    
    zip_buffer = BytesIO()
    with ZipFile(zip_buffer, 'w') as zip_file:
        for i in range(1, df_completo['FOLHA'].max() + 1):
            folha_df = df_completo[df_completo['FOLHA'] == i][colunas_disponiveis]
            csv_buffer = BytesIO()
            # Uso de 'utf-8-sig' para garantir compatibilidade com Excel em português
            folha_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            csv_buffer.seek(0)
            zip_file.writestr(f'Folha_Trabalho_{i}.csv', csv_buffer.getvalue())
    
    zip_buffer.seek(0)
    return zip_buffer.read()

def extrair_cils_do_xlsx(arquivo_xlsx):
    try:
        # Usa BytesIO para permitir o seek (rebobinar o arquivo) se necessário
        arquivo_xlsx.seek(0)
        df = pd.read_excel(arquivo_xlsx)
        coluna_cil = None
        
        # Tentativa de identificar a coluna de CIL
        for col in df.columns:
            if any(termo in str(col).lower() for termo in ['cil', 'código', 'codigo', 'numero']):
                coluna_cil = col
                break
        
        # Fallback para a primeira coluna
        if coluna_cil is None:
            coluna_cil = df.columns[0]
            
        cils = df[coluna_cil].dropna().astype(str).str.strip()
        # Remove cabeçalhos se estiverem misturados (embora read_excel deva lidar com isso)
        cils = cils[~cils.str.lower().isin(['cil', 'cils', 'código', 'codigo', 'nome', 'numero', 'número', ''])]
        
        # Filtra valores vazios ou 'nan' e retorna lista de únicos
        return list(set([cil for cil in cils.tolist() if cil and cil != 'nan']))
        
    except Exception as e:
        # st.warning(f"Erro ao extrair CILs do XLSX: {e}")
        return []

# --- PÁGINAS COM APPROACH ESTÁTICO ---
def render_login_page(db_manager):
    """Renderização estática da página de login"""
    st.title("🔐 Sistema de Gestão de Dados")
    
    # Container estático para login
    with st.container():
        st.markdown("### Login de Acesso")
        
        with st.form("login_form_static"):
            username = st.text_input("Nome de Usuário", key="static_username")
            password = st.text_input("Senha", type="password", key="static_password")
            
            # ATENÇÃO: Uso de st.rerun() em vez de st.experimental_rerun() e remoção do time.sleep(1)
            if st.form_submit_button("Entrar no Sistema", use_container_width=True):
                if username and password:
                    user_info = db_manager.autenticar_usuario(username, password)
                    if user_info:
                        st.session_state.authenticated = True
                        st.session_state.user = user_info
                        st.session_state.current_page = 'manager'
                        st.success("✅ Login realizado com sucesso!")
                        # Remove time.sleep(1) para evitar problemas de sincronização do DOM
                        st.rerun() # Força o rerun imediatamente
                    else:
                        st.error("❌ Credenciais inválidas")
                else:
                    st.warning("⚠️ Preencha todos os campos")

def render_manager_page(db_manager):
    """Renderização estática da página principal"""
    user = st.session_state.user
    
    # Sidebar estática
    with st.sidebar:
        st.markdown(f"**👤 Usuário:** {user['nome']}")
        st.markdown(f"**🎯 Função:** {user['role']}")
        st.markdown("---")
        
        if st.button("🚪 Sair do Sistema", use_container_width=True):
            st.session_state.authenticated = False
            st.session_state.user = None
            st.session_state.current_page = 'login'
            # ATENÇÃO: Uso de st.rerun()
            st.rerun()
        
        st.markdown("---")
        with st.expander("🔐 Alterar Senha"):
            with st.form("alterar_senha_sidebar"):
                nova_senha = st.text_input("Nova Senha", type="password", key="nova_senha_sidebar")
                confirmar_senha = st.text_input("Confirmar Senha", type="password", key="confirmar_senha_sidebar")
                if st.form_submit_button("Alterar Senha", use_container_width=True):
                    if nova_senha == confirmar_senha and nova_senha:
                        sucesso, msg = db_manager.alterar_senha(user['id'], nova_senha)
                        if sucesso:
                            st.success("✅ Senha alterada! Faça o login novamente.")
                            st.session_state.authenticated = False # Força o re-login após a troca
                            st.session_state.user = None
                            st.rerun()
                        else:
                            st.error(f"❌ {msg}")
                    else:
                        st.error("❌ Senhas não coincidem ou campo está vazio")

    # Conteúdo principal estático
    st.title(f"📊 Bem-vindo, {user['nome']}!")
    
    # Abas estáticas baseadas na role
    if user['role'] == 'Administrador':
        tabs = st.radio("Navegação:", ["📋 Folhas de Trabalho", "📥 Importação", "👥 Usuários", "🔄 Reset"], horizontal=True)
    else:
        # Impede que usuários não-admin vejam as abas de admin
        tabs_options = ["📋 Folhas de Trabalho"]
        tabs = st.radio("Navegação:", tabs_options, horizontal=True) 
    
    # Conteúdo das abas
    if tabs == "📋 Folhas de Trabalho":
        render_folhas_trabalho(db_manager, user)
    elif tabs == "📥 Importação" and user['role'] == 'Administrador':
        render_importacao(db_manager)
    elif tabs == "👥 Usuários" and user['role'] == 'Administrador':
        render_usuarios(db_manager)
    elif tabs == "🔄 Reset" and user['role'] == 'Administrador':
        render_reset(db_manager)

def render_folhas_trabalho(db_manager, user):
    """Renderização estática da geração de folhas"""
    st.header("📝 Geração de Folhas de Trabalho")
    
    with st.container():
        tipo_selecionado = st.radio("Tipo de Geração:", ["PT", "LOCALIDADE", "AVULSO"], horizontal=True)
        
        valor_selecionado = None
        arquivo_xlsx = None
        
        if tipo_selecionado in ["PT", "LOCALIDADE"]:
            # Obtendo valores no início do rerun
            valores = db_manager.obter_valores_unicos(tipo_selecionado.lower())
            if valores:
                valor_selecionado = st.selectbox(f"Selecione {tipo_selecionado}:", ["Selecione..."] + valores)
                if valor_selecionado == "Selecione...":
                    valor_selecionado = None
                    
        elif tipo_selecionado == "AVULSO":
            arquivo_xlsx = st.file_uploader("📋 Upload de arquivo XLSX com CILs:", type=["xlsx"])
            if arquivo_xlsx:
                cils = extrair_cils_do_xlsx(arquivo_xlsx)
                if cils:
                    st.info(f"📊 {len(cils)} CIL(s) identificados no arquivo.")
        
        col1, col2 = st.columns(2)
        with col1:
            num_nibs = st.number_input("NIBs por folha:", min_value=1, value=50, key="nibs_por_folha")
        with col2:
            max_folhas = st.number_input("Máx. folhas:", min_value=1, value=10, key="max_folhas")
        
        if st.button("🔄 Gerar Folhas", use_container_width=True):
            
            cils_validos = None
            if tipo_selecionado == "AVULSO":
                if not arquivo_xlsx:
                    st.error("Faça upload do arquivo XLSX")
                    return
                cils_validos = extrair_cils_do_xlsx(arquivo_xlsx)
                if not cils_validos:
                    st.error("Nenhum CIL válido encontrado no arquivo.")
                    return
            
            elif not valor_selecionado:
                st.error("Selecione um valor válido.")
                return

            # Se tudo estiver ok, prossegue
            with st.spinner("Gerando folhas..."):
                df_folhas, _ = db_manager.gerar_folhas_trabalho(
                    tipo_selecionado, valor_selecionado, max_folhas, num_nibs, cils_validos
                )
                    
            if df_folhas is not None and not df_folhas.empty:
                st.success(f"✅ {df_folhas['FOLHA'].max()} folhas geradas, total de {len(df_folhas)} registros.")
                
                zip_data = generate_csv_zip(df_folhas)
                st.download_button(
                    label=f"📦 Baixar ZIP com {df_folhas['FOLHA'].max()} Folha(s)",
                    data=zip_data,
                    file_name=f"folhas_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.zip",
                    mime="application/zip",
                    use_container_width=True
                )
            else:
                st.warning("⚠️ Nenhuma folha gerada (todos os registros podem estar em 'prog' ou não há dados para o filtro).")

def render_importacao(db_manager):
    """Renderização estática da importação"""
    st.header("📥 Importação de Dados")
    
    with st.container():
        st.warning("⚠️ A importação **substituirá todos** os dados existentes na tabela `bd`!")
        
        uploaded_file = st.file_uploader("Selecione arquivo CSV:", type=["csv"])
        
        if uploaded_file and st.button("🚀 Importar Dados", use_container_width=True):
            uploaded_file.seek(0) # Garante que o arquivo está no início
            with st.spinner("Importando..."):
                if db_manager.importar_csv(uploaded_file):
                    st.success("✅ Importação concluída!")
                else:
                    st.error("❌ Falha na importação. Verifique se o arquivo possui as 31 colunas esperadas.")

def render_usuarios(db_manager):
    """Renderização estática do gerenciamento de usuários"""
    st.header("👥 Gerenciamento de Usuários")
    
    with st.container():
        with st.expander("➕ Novo Usuário"):
            # Usando form para evitar reruns parciais durante a criação
            with st.form("novo_usuario_form"):
                username = st.text_input("Username", key="new_user_username")
                nome = st.text_input("Nome completo", key="new_user_nome")
                password = st.text_input("Senha", type="password", key="new_user_password")
                role = st.selectbox("Função", ["Administrador", "Assistente Administrativo", "Técnico"], key="new_user_role")
                
                if st.form_submit_button("Criar Usuário", use_container_width=True):
                    if username and password:
                        sucesso, msg = db_manager.criar_usuario(username, password, nome, role)
                        if sucesso:
                            st.success("✅ " + msg)
                            st.rerun() # Rerun para atualizar a lista de usuários
                        else:
                            st.error("❌ " + msg)
                    else:
                        st.error("Preencha todos os campos obrigatórios.")
        
        st.markdown("---")
        st.subheader("Usuários Existentes")
        
        # Obter usuários em cada rerun
        usuarios = db_manager.obter_usuarios()
        
        # Usar um formulário pai para englobar todas as edições para um único submit global
        # Isso pode ser simplificado, mas no Streamlit 1.x, botões separados dentro de expanders
        # requerem reruns para mostrar o sucesso/erro, o que está ok aqui.
        for user in usuarios:
            # Usando o ID do usuário como parte da chave para garantir unicidade
            with st.expander(f"👤 {user[1]} - {user[3]} (ID: {user[0]})"):
                col1, col2 = st.columns(2)
                
                # Campos de Edição
                with col1:
                    novo_nome = st.text_input("Nome", value=user[2], key=f"nome_edit_{user[0]}")
                    nova_role = st.selectbox(
                        "Função", 
                        ["Administrador", "Assistente Administrativo", "Técnico"],
                        index=["Administrador", "Assistente Administrativo", "Técnico"].index(user[3]),
                        key=f"role_edit_{user[0]}"
                    )
                    
                    if st.button("💾 Salvar Edição", key=f"save_edit_{user[0]}", use_container_width=True):
                        sucesso, msg = db_manager.editar_usuario(user[0], novo_nome, nova_role)
                        if sucesso:
                            st.success("✅ " + msg)
                            st.rerun() # Rerun para atualizar a exibição do usuário
                        else:
                            st.error("❌ " + msg)

                # Campo de Senha e Exclusão
                with col2:
                    nova_senha = st.text_input("Nova senha (opcional)", type="password", key=f"pass_edit_{user[0]}")
                    if st.button("🔑 Alterar Senha", key=f"pass_btn_{user[0]}", use_container_width=True, disabled=not nova_senha):
                        if nova_senha:
                            sucesso, msg = db_manager.alterar_senha(user[0], nova_senha)
                            if sucesso:
                                st.success("✅ " + msg)
                                st.rerun()
                            else:
                                st.error("❌ " + msg)
                    
                    # Botão de exclusão (com confirmação implícita)
                    if st.button("🗑️ Excluir Usuário", key=f"del_btn_{user[0]}", use_container_width=True):
                        if st.session_state.user['id'] == user[0]:
                            st.error("❌ Você não pode excluir seu próprio usuário!")
                        else:
                            sucesso, msg = db_manager.excluir_usuario(user[0])
                            if sucesso:
                                st.success("✅ " + msg)
                                st.rerun() # Rerun para remover o usuário da lista
                            else:
                                st.error("❌ " + msg)

def render_reset(db_manager):
    """Renderização estática do reset"""
    st.header("🔄 Reset de Estado")
    
    with st.container():
        st.warning("⚠️ O reset irá alterar o estado 'prog' de volta para 'vazio' para os registros filtrados, permitindo que sejam gerados novamente.")
        
        tipo_reset = st.selectbox("Tipo de reset:", ["PT", "LOCALIDADE", "AVULSO"], key="tipo_reset_select")
        
        valor_reset = ""
        if tipo_reset in ["PT", "LOCALIDADE"]:
            valores = db_manager.obter_valores_unicos(tipo_reset.lower())
            # Filtra valores em "prog"
            valores_prog = db_manager.obter_valores_unicos(tipo_reset.lower(), filtro_estado='prog') 
            
            if valores:
                # Use os valores que estão em 'prog' para o reset ser mais intuitivo
                if not valores_prog:
                    st.info(f"Nenhum registro encontrado em 'prog' para {tipo_reset}.")
                    valor_reset = "Nenhum em 'prog'"
                else:
                    valor_reset = st.selectbox(f"Valor de {tipo_reset} (com registros em 'prog'):", ["Selecione..."] + valores_prog, key="valor_reset_select")
                    
        # Não precisa de selectbox para AVULSO, pois reseta todos em 'prog'
        if tipo_reset == "AVULSO":
             st.info("O modo AVULSO resetará *todos* os registros que estiverem com o estado 'prog'.")

        
        if st.button("🔴 Executar Reset", use_container_width=True):
            if tipo_reset in ["PT", "LOCALIDADE"] and (valor_reset == "Selecione..." or valor_reset == "Nenhum em 'prog'"):
                st.error("Selecione um valor válido ou mude o Tipo de reset.")
            else:
                with st.spinner("Executando reset..."):
                    sucesso, resultado = db_manager.resetar_estado(tipo_reset, valor_reset)
                    if sucesso:
                        st.success(f"✅ Reset concluído. {resultado} registros afetados.")
                        st.rerun()
                    else:
                        st.error("❌ Falha no reset")

# --- FUNÇÃO PRINCIPAL SUPER ESTÁTICA ---
def main():
    """Função principal com approach completamente estático"""
    
    # Inicialização única do database manager
    if 'db_manager' not in st.session_state:
        try:
            # A URL deve vir de st.secrets e ser acessível
            st.session_state.db_manager = PostgresDatabaseManager(POSTGRES_URL)
        except Exception as e:
            st.error(f"❌ Falha crítica na conexão com o banco. Verifique as credenciais em st.secrets e a conexão: {e}")
            return
    
    # Renderização condicional SUPER SIMPLES
    if not st.session_state.authenticated:
        render_login_page(st.session_state.db_manager)
    else:
        render_manager_page(st.session_state.db_manager)

if __name__ == '__main__':
    main()