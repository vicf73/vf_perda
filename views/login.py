# -*- coding: utf-8 -*-
import streamlit as st
import logging

logger = logging.getLogger(__name__)

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
