# -*- coding: utf-8 -*-
import sys
import os
import streamlit as st
import logging

# --- AJUSTE DE PATH E DEBUG PARA NUVEM ---
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

# Debug: Listar arquivos na raiz (visível apenas nos logs do 'Manage app')
logger = logging.getLogger(__name__)
try:
    logger.info(f"Diretório atual: {current_dir}")
    logger.info(f"Arquivos na raiz: {os.listdir(current_dir)}")
    if os.path.exists(os.path.join(current_dir, 'views')):
        logger.info(f"Arquivos em 'views/': {os.listdir(os.path.join(current_dir, 'views'))}")
except Exception as e:
    logger.error(f"Erro ao listar diretórios: {e}")

from database import PostgresDatabaseManager, POSTGRES_URL
try:
    from views.login import login_page
    from views.admin import manager_page
except ModuleNotFoundError as e:
    st.error(f"❌ Erro Crítico de Importação: {e}")
    st.write("### Diagnóstico do Sistema (GitHub vs Local)")
    st.write(f"**Diretório Atual:** `{current_dir}`")
    try:
        arquivos_raiz = os.listdir(current_dir)
        st.write(f"**Arquivos na Raiz:** `{arquivos_raiz}`")
        if 'views' in arquivos_raiz:
            arquivos_views = os.listdir(os.path.join(current_dir, 'views'))
            st.write(f"**Arquivos em 'views/':** `{arquivos_views}`")
            if '__init__.py' not in arquivos_views:
                st.warning("⚠️ O arquivo `__init__.py` NÃO foi encontrado dentro da pasta `views`.")
        else:
            st.warning("⚠️ A pasta `views/` não foi encontrada na raiz do projeto no GitHub.")
    except Exception as list_err:
        st.error(f"Erro ao listar pastas: {list_err}")
    
    st.info("📌 Dica: O arquivo deve se chamar exatamente `__init__.py` (2 underlines + init + 2 underlines).")
    st.stop()
import utils

# Configuração da página - DEVE SER A PRIMEIRA CHAMADA STREAMLIT
st.set_page_config(
    page_title="Sistema de Gestão de Dados - V.Ferreira",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Função principal do aplicativo Streamlit."""
    
    # Inicialização do Estado de Sessão
    if 'authenticated' not in st.session_state:
        st.session_state['authenticated'] = False
        st.session_state['user'] = None

    # Configuração do DB
    try:
        db_manager = PostgresDatabaseManager(POSTGRES_URL)
        
        # Mostrar status da conexão no sidebar apenas se autenticado
        if st.session_state['authenticated']:
            try:
                # Teste simples de conexão
                with db_manager.engine.connect() as conn:
                    from sqlalchemy import text
                    count_result = conn.execute(text("SELECT COUNT(*) FROM bd"))
                    record_count = count_result.scalar()
                    st.sidebar.success(f"✅ Conectado ao PostgreSQL")
                    st.sidebar.info(f"📊 Registros na BD: {record_count:,}")
            except Exception as e:
                st.sidebar.error(f"⚠️ Aviso de conexão: {e}")
            
    except Exception as e:
        st.error(f"O aplicativo não pôde se conectar ao banco de dados. Verifique a configuração.")
        logger.error(f"Falha na inicialização do banco de dados: {e}")
        return

    # Limpeza de sessão removida para evitar conflitos com widgets
    # utils.clean_session_state()

    # Roteamento Principal
    if st.session_state['authenticated']:
        manager_page(db_manager)
    else:
        login_page(db_manager)

if __name__ == '__main__':
    main()