# -*- coding: utf-8 -*-
import sys
import os
import streamlit as st
import logging

# Garantir que o diretório raiz está no path para deploys em nuvem
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from database import PostgresDatabaseManager, POSTGRES_URL
from views.login import login_page
from views.admin import manager_page

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
                with db_manager.engine.connect() as conn:
                    from sqlalchemy import text
                    count_result = conn.execute(text("SELECT COUNT(*) FROM bd"))
                    record_count = count_result.scalar()
                    st.sidebar.success(f"✅ Conectado ao Banco de Dados")
                    st.sidebar.info(f"📊 Registros na BD: {record_count:,}")
            except Exception as e:
                st.sidebar.error(f"⚠️ Aviso de conexão: {e}")
            
    except Exception as e:
        st.error(f"O aplicativo não pôde se conectar ao banco de dados. Verifique os Secrets.")
        logger.error(f"Falha na inicialização do banco de dados: {e}")
        return

    # Roteamento Principal
    if st.session_state['authenticated']:
        manager_page(db_manager)
    else:
        login_page(db_manager)

if __name__ == '__main__':
    main()
