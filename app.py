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

# --- CONFIGURAÇÃO SIMPLIFICADA ---
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

# CSS MINIMALISTA - apenas o essencial
st.markdown("""
<style>
    /* Apenas esconder elementos, sem manipulações agressivas */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
    
    /* Layout básico sem transições problemáticas */
    .main .block-container {
        padding-top: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# --- INICIALIZAÇÃO SIMPLES ---
if 'app_initialized' not in st.session_state:
    st.session_state.app_initialized = True
    st.session_state.authenticated = False
    st.session_state.user = None
    st.session_state.current_view = 'login'

# Para PostgreSQL
try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    st.error("❌ Dependências não instaladas.")
    st.stop()

# --- CONFIGURAÇÃO BANCO ---
try:
    POSTGRES_URL = f"postgresql+psycopg2://{st.secrets['postgres']['user']}:{st.secrets['postgres']['password']}@{st.secrets['postgres']['host']}:{st.secrets['postgres']['port']}/{st.secrets['postgres']['database']}"
except Exception:
    st.error("❌ Erro nas credenciais do banco.")
    st.stop()

# --- DATABASE MANAGER (MANTIDO SIMPLES) ---
class DatabaseManager:
    def __init__(self, database_url):
        try:
            self.engine = create_engine(database_url)
            self._init_db()
        except Exception as e:
            st.error(f"❌ Erro PostgreSQL: {e}")
            raise

    def _init_db(self):
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
                default_users = [
                    ('Admin', self._hash_password('admin123'), 'Administrador', 'Administrador'),
                    ('AssAdm', self._hash_password('adm123'), 'Assistente', 'Assistente Administrativo')
                ]
                for user in default_users:
                    conn.execute(
                        text("INSERT INTO usuarios (username, password_hash, nome, role) VALUES (:u, :p, :n, :r)"),
                        {"u": user[0], "p": user[1], "n": user[2], "r": user[3]}
                    )
            conn.commit()

    @staticmethod
    def _hash_password(password):
        return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

    def authenticate(self, username, password):
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT id, username, password_hash, nome, role FROM usuarios WHERE username = :u"),
                {"u": username}
            )
            user = result.fetchone()
        
        if user and bcrypt.checkpw(password.encode('utf-8'), user[2].encode('utf-8')):
            return {'id': user[0], 'username': user[1], 'nome': user[3], 'role': user[4]}
        return None

    def get_users(self):
        with self.engine.connect() as conn:
            df = pd.read_sql_query(text("SELECT id, username, nome, role FROM usuarios ORDER BY username"), conn)
        return df.to_dict('records')

    def create_user(self, username, password, nome, role):
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text("INSERT INTO usuarios (username, password_hash, nome, role) VALUES (:u, :p, :n, :r)"),
                    {"u": username, "p": self._hash_password(password), "n": nome, "r": role}
                )
                conn.commit()
            return True, "Usuário criado com sucesso"
        except SQLAlchemyError as e:
            return False, str(e)

    def update_user(self, user_id, nome, role):
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("UPDATE usuarios SET nome = :n, role = :r WHERE id = :id"),
                    {"n": nome, "r": role, "id": user_id}
                )
                conn.commit()
            return result.rowcount > 0, "Usuário atualizado"
        except SQLAlchemyError as e:
            return False, str(e)

    def change_password(self, user_id, new_password):
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("UPDATE usuarios SET password_hash = :p WHERE id = :id"),
                    {"p": self._hash_password(new_password), "id": user_id}
                )
                conn.commit()
            return result.rowcount > 0, "Senha alterada"
        except SQLAlchemyError as e:
            return False, str(e)

    def delete_user(self, user_id):
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("DELETE FROM usuarios WHERE id = :id"), {"id": user_id})
                conn.commit()
            return result.rowcount > 0, "Usuário excluído"
        except SQLAlchemyError as e:
            return False, str(e)

    def import_csv(self, arquivo_csv):
        try:
            raw_data = arquivo_csv.getvalue()
            encoding = chardet.detect(raw_data)['encoding'] or 'utf-8'
            
            df = pd.read_csv(arquivo_csv, encoding=encoding, header=None, low_memory=False)
            
            if len(df.columns) < 31:
                return False
                
            columns = ['cil', 'prod', 'contador', 'leitura', 'mat_contador', 'med_fat', 'qtd', 'valor', 'situacao', 'acordo',
                      'nib', 'seq', 'localidade', 'pt', 'desv', 'mat_leitura', 'desc_uni', 'est_contr', 'anomalia', 'id',
                      'produto', 'nome', 'criterio', 'desc_tp_cli', 'tip', 'tarifa', 'modelo', 'lat', 'long', 'fraud', 'estado']
            
            df.columns = columns[:len(df.columns)]
            
            for col in ['criterio', 'pt', 'localidade']:
                if col in df.columns:
                    df[col] = df[col].fillna('').astype(str).str.upper().str.strip()
            
            df['estado'] = df['estado'].fillna('').astype(str).str.lower().str.strip()
            df['qtd'] = pd.to_numeric(df['qtd'], errors='coerce').fillna(0)
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce').fillna(0)
            
            with self.engine.connect() as conn:
                df.to_sql('bd', conn, if_exists='replace', index=False)
                conn.commit()
            
            return True
            
        except Exception:
            return False

    def get_unique_values(self, column):
        try:
            with self.engine.connect() as conn:
                query = text(f"SELECT DISTINCT UPPER(TRIM({column})) as val FROM bd WHERE {column} IS NOT NULL AND TRIM({column}) != '' ORDER BY val")
                df = pd.read_sql_query(query, conn)
                return df['val'].tolist()
        except Exception:
            return []

    def generate_work_sheets(self, sheet_type, selected_value, max_sheets, nibs_per_sheet, valid_cils=None):
        try:
            with self.engine.connect() as conn:
                where_conditions = ["UPPER(TRIM(criterio)) = 'SUSP'", "COALESCE(estado, '') != 'prog'"]
                params = {}
                
                if sheet_type == "AVULSO" and valid_cils:
                    where_conditions.append("cil = ANY(:cils)")
                    params['cils'] = valid_cils
                elif selected_value:
                    col = 'pt' if sheet_type == "PT" else 'localidade'
                    where_conditions.append(f"UPPER(TRIM({col})) = :val")
                    params['val'] = selected_value.upper().strip()
                
                query = text(f"SELECT * FROM bd WHERE {' AND '.join(where_conditions)} ORDER BY nib")
                df = pd.read_sql_query(query, conn, params=params)
                
                if df.empty:
                    return None, []
                
                df['nib'] = df['nib'].fillna('').astype(str).str.strip()
                valid_nibs = [n for n in df['nib'].unique() if n]
                
                if not valid_nibs:
                    return None, []
                
                sheets = []
                total_sheets = min(max_sheets, (len(valid_nibs) + nibs_per_sheet - 1) // nibs_per_sheet)
                
                for i in range(total_sheets):
                    nibs_in_sheet = valid_nibs[i * nibs_per_sheet:(i + 1) * nibs_per_sheet]
                    sheet_df = df[df['nib'].isin(nibs_in_sheet)].copy()
                    sheet_df['FOLHA'] = i + 1
                    sheets.append(sheet_df)
                    
                    update_query = text("UPDATE bd SET estado = 'prog' WHERE nib = ANY(:nibs)")
                    conn.execute(update_query, {'nibs': nibs_in_sheet})
                
                conn.commit()
                
                if sheets:
                    return pd.concat(sheets, ignore_index=True), []
                return None, []
                
        except Exception:
            return None, []

    def reset_state(self, reset_type, value):
        try:
            with self.engine.connect() as conn:
                if reset_type == 'PT':
                    query = text("UPDATE bd SET estado = '' WHERE estado = 'prog' AND UPPER(TRIM(pt)) = :v")
                elif reset_type == 'LOCALIDADE':
                    query = text("UPDATE bd SET estado = '' WHERE estado = 'prog' AND UPPER(TRIM(localidade)) = :v")
                else:
                    query = text("UPDATE bd SET estado = '' WHERE estado = 'prog'")
                
                result = conn.execute(query, {"v": value.upper().strip()} if reset_type != 'AVULSO' else {})
                conn.commit()
                return True, result.rowcount
                
        except Exception:
            return False, 0

# --- FUNÇÕES AUXILIARES ---
def extract_cils_from_xlsx(file):
    try:
        df = pd.read_excel(file)
        for col in df.columns:
            if any(keyword in str(col).lower() for keyword in ['cil', 'codigo', 'código', 'numero']):
                cils = df[col].dropna().astype(str).str.strip()
                valid_cils = [c for c in cils.unique() if c and c.lower() not in ['cil', 'cils', 'codigo', 'código', 'nome']]
                return valid_cils
        cils = df.iloc[:, 0].dropna().astype(str).str.strip()
        return [c for c in cils.unique() if c and c.lower() not in ['cil', 'cils', 'codigo', 'código', 'nome']]
    except Exception:
        return []

def create_zip_files(df):
    if df is None or df.empty:
        return None
        
    cols_to_export = ['cil', 'prod', 'contador', 'leitura', 'mat_contador', 'med_fat', 'qtd', 'valor', 'situacao', 'acordo']
    available_cols = [c for c in cols_to_export if c in df.columns]
    
    zip_buffer = BytesIO()
    with ZipFile(zip_buffer, 'w') as zip_file:
        for sheet_num in range(1, df['FOLHA'].max() + 1):
            sheet_data = df[df['FOLHA'] == sheet_num][available_cols]
            csv_buffer = BytesIO()
            sheet_data.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            csv_buffer.seek(0)
            zip_file.writestr(f'Folha_{sheet_num}.csv', csv_buffer.getvalue())
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

# --- APLICAÇÃO PRINCIPAL COM CORREÇÕES ---

def main():
    """Função principal com as correções aplicadas"""
    
    # Inicialização do database manager
    if 'db' not in st.session_state:
        try:
            st.session_state.db = DatabaseManager(POSTGRES_URL)
        except Exception:
            st.error("❌ Falha na inicialização do banco")
            return
    
    # Renderização condicional
    if not st.session_state.authenticated:
        render_login_screen()
    else:
        render_main_application()

def render_login_screen():
    """Tela de login com correção do rerun"""
    
    with st.container():
        st.title("🔐 Sistema de Gestão - V.Ferreira")
        st.markdown("---")
        
        with st.form("login_form", clear_on_submit=False):
            username = st.text_input("👤 Nome de usuário")
            password = st.text_input("🔒 Senha", type="password")
            
            submitted = st.form_submit_button("🚀 Entrar no Sistema", use_container_width=True)
            
            if submitted:
                if username and password:
                    user_info = st.session_state.db.authenticate(username, password)
                    if user_info:
                        st.session_state.authenticated = True
                        st.session_state.user = user_info
                        st.success("✅ Login realizado com sucesso!")
                        # CORREÇÃO APLICADA: st.rerun() sem time.sleep
                        st.rerun()
                    else:
                        st.error("❌ Credenciais inválidas")
                else:
                    st.warning("⚠️ Preencha todos os campos")

def render_main_application():
    """Aplicação principal"""
    
    user = st.session_state.user
    
    # Sidebar
    with st.sidebar:
        st.markdown(f"### 👤 {user['nome']}")
        st.markdown(f"**Função:** {user['role']}")
        st.markdown("---")
        
        if user['role'] == 'Administrador':
            options = ["📋 Folhas", "📥 Importar", "👥 Usuários", "🔄 Reset", "🔐 Senha"]
            selected = st.radio("Navegação:", options, key="main_nav")
        else:
            selected = "📋 Folhas"
        
        st.markdown("---")
        
        # CORREÇÃO APLICADA: st.rerun() sem time.sleep no logout
        if st.button("🚪 Sair do Sistema", use_container_width=True, key="logout_btn"):
            st.session_state.authenticated = False
            st.session_state.user = None
            st.success("✅ Logout realizado!")
            st.rerun()
    
    # Conteúdo principal
    if selected == "📋 Folhas":
        render_work_sheets()
    elif selected == "📥 Importar" and user['role'] == 'Administrador':
        render_import_section()
    elif selected == "👥 Usuários" and user['role'] == 'Administrador':
        render_user_management()
    elif selected == "🔄 Reset" and user['role'] == 'Administrador':
        render_reset_section()
    elif selected == "🔐 Senha":
        render_password_change()

def render_work_sheets():
    st.header("📝 Gerar Folhas de Trabalho")
    
    with st.container():
        sheet_type = st.radio("Tipo de geração:", ["PT", "LOCALIDADE", "AVULSO"], 
                            horizontal=True, key="sheet_type_radio")
        
        selected_value = None
        uploaded_file = None
        
        if sheet_type in ["PT", "LOCALIDADE"]:
            values = st.session_state.db.get_unique_values(sheet_type.lower())
            if values:
                selected_value = st.selectbox(f"Selecione {sheet_type}:", [""] + values,
                                            key=f"select_{sheet_type}")
        
        elif sheet_type == "AVULSO":
            uploaded_file = st.file_uploader("📋 Upload de arquivo com CILs:", 
                                           type=["xlsx"], key="cil_uploader")
            if uploaded_file:
                cils = extract_cils_from_xlsx(uploaded_file)
                if cils:
                    st.info(f"📊 {len(cils)} CIL(s) encontrados")
        
        col1, col2 = st.columns(2)
        with col1:
            nibs_per_sheet = st.number_input("NIBs por folha:", min_value=1, value=50,
                                           key="nibs_input")
        with col2:
            max_sheets = st.number_input("Máximo de folhas:", min_value=1, value=10,
                                       key="max_sheets_input")
        
        if st.button("🔄 Gerar Folhas", use_container_width=True, key="generate_btn"):
            if sheet_type != "AVULSO" and not selected_value:
                st.error("Selecione um valor válido")
            elif sheet_type == "AVULSO" and not uploaded_file:
                st.error("Faça upload do arquivo")
            else:
                cils = extract_cils_from_xlsx(uploaded_file) if sheet_type == "AVULSO" else None
                
                with st.spinner("Gerando folhas..."):
                    result_df, _ = st.session_state.db.generate_work_sheets(
                        sheet_type, selected_value, max_sheets, nibs_per_sheet, cils
                    )
                
                if result_df is not None:
                    st.success(f"✅ {result_df['FOLHA'].max()} folha(s) gerada(s)!")
                    
                    zip_data = create_zip_files(result_df)
                    if zip_data:
                        st.download_button(
                            label="📦 Baixar ZIP com Folhas",
                            data=zip_data,
                            file_name=f"folhas_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.zip",
                            mime="application/zip",
                            use_container_width=True,
                            key="download_zip_btn"
                        )
                else:
                    st.warning("⚠️ Nenhuma folha gerada - verifique os critérios")

def render_import_section():
    st.header("📥 Importar Dados CSV")
    
    with st.container():
        st.warning("⚠️ Esta operação substituirá TODOS os dados existentes!")
        
        uploaded_file = st.file_uploader("Selecione o arquivo CSV:", 
                                       type=["csv"], key="csv_uploader_import")
        
        if uploaded_file and st.button("🚀 Importar Dados", 
                                     use_container_width=True, key="import_btn"):
            with st.spinner("Importando..."):
                success = st.session_state.db.import_csv(uploaded_file)
            
            if success:
                st.success("✅ Dados importados com sucesso!")
            else:
                st.error("❌ Falha na importação - verifique o arquivo")

def render_user_management():
    st.header("👥 Gerenciar Usuários")
    
    with st.container():
        with st.expander("➕ Adicionar Novo Usuário", key="new_user_expander"):
            with st.form("new_user_form", key="new_user_form_main"):
                col1, col2 = st.columns(2)
                with col1:
                    new_user = st.text_input("Username", key="new_username")
                    new_name = st.text_input("Nome completo", key="new_fullname")
                with col2:
                    new_pass = st.text_input("Senha", type="password", key="new_password")
                    new_role = st.selectbox("Função", ["Administrador", "Assistente Administrativo", "Técnico"],
                                          key="new_role_select")
                
                if st.form_submit_button("Criar Usuário", use_container_width=True, key="create_user_btn"):
                    if new_user and new_pass:
                        success, msg = st.session_state.db.create_user(new_user, new_pass, new_name, new_role)
                        if success:
                            st.success("✅ Usuário criado!")
                            st.rerun()
                        else:
                            st.error(f"❌ {msg}")
        
        st.markdown("---")
        st.subheader("Usuários Existentes")
        
        users = st.session_state.db.get_users()
        for user_data in users:
            with st.expander(f"👤 {user_data['username']} - {user_data['role']}", 
                           key=f"user_expander_{user_data['id']}"):
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    edited_name = st.text_input("Nome", value=user_data['nome'], 
                                              key=f"name_{user_data['id']}")
                    edited_role = st.selectbox(
                        "Função", 
                        ["Administrador", "Assistente Administrativo", "Técnico"],
                        index=["Administrador", "Assistente Administrativo", "Técnico"].index(user_data['role']),
                        key=f"role_{user_data['id']}"
                    )
                
                with col2:
                    if st.button("💾 Salvar", key=f"save_{user_data['id']}"):
                        success, msg = st.session_state.db.update_user(user_data['id'], edited_name, edited_role)
                        if success:
                            st.success("✅ Alterações salvas!")
                            st.rerun()
                        else:
                            st.error(f"❌ {msg}")
                    
                    new_password = st.text_input("Nova senha", type="password", 
                                               key=f"pass_{user_data['id']}")
                    if st.button("🔑 Alterar Senha", key=f"chpass_{user_data['id']}"):
                        if new_password:
                            success, msg = st.session_state.db.change_password(user_data['id'], new_password)
                            if success:
                                st.success("✅ Senha alterada!")
                                st.rerun()
                            else:
                                st.error(f"❌ {msg}")

def render_reset_section():
    st.header("🔄 Resetar Estado")
    
    with st.container():
        reset_type = st.selectbox("Tipo de reset:", ["PT", "LOCALIDADE", "AVULSO"],
                                key="reset_type_select")
        
        reset_value = ""
        if reset_type in ["PT", "LOCALIDADE"]:
            values = st.session_state.db.get_unique_values(reset_type.lower())
            if values:
                reset_value = st.selectbox(f"Valor de {reset_type}:", [""] + values,
                                         key=f"reset_value_{reset_type}")
        
        if st.button("🔴 Executar Reset", use_container_width=True, key="reset_execute_btn"):
            success, count = st.session_state.db.reset_state(reset_type, reset_value)
            if success:
                st.success(f"✅ Reset concluído! {count} registro(s) afetado(s).")
            else:
                st.error("❌ Falha no reset")

def render_password_change():
    st.header("🔐 Alterar Minha Senha")
    
    with st.container():
        with st.form("change_my_password", key="change_pass_form"):
            current_user = st.session_state.user
            
            st.markdown(f"**Alterando senha para:** {current_user['nome']}")
            
            new_pass = st.text_input("Nova senha", type="password", key="new_pass_personal")
            confirm_pass = st.text_input("Confirmar senha", type="password", key="confirm_pass_personal")
            
            if st.form_submit_button("🔄 Alterar Minha Senha", use_container_width=True, key="change_pass_btn"):
                if new_pass and confirm_pass:
                    if new_pass == confirm_pass:
                        success, msg = st.session_state.db.change_password(current_user['id'], new_pass)
                        if success:
                            st.success("✅ Senha alterada com sucesso!")
                        else:
                            st.error(f"❌ {msg}")
                    else:
                        st.error("❌ As senhas não coincidem")
                else:
                    st.error("❌ Preencha todos os campos")

# --- EXECUÇÃO PRINCIPAL ---
if __name__ == '__main__':
    main()