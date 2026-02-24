# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import datetime
import logging
import utils
from views.dashboard import mostrar_dashboard_geral
from views.reports import mostrar_relatorio_operacional, mostrar_analise_eficiencia, mostrar_relatorio_usuarios

logger = logging.getLogger(__name__)

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

        # --- Histórico de Geração ---
        with st.expander("📜 Histórico Recente de Gerações"):
            historico = db_manager.obter_historico_geracao()
            if not historico.empty:
                st.dataframe(historico, use_container_width=True)
            else:
                st.info("Nenhum histórico de geração encontrado.")

        tipos_folha = ["PT", "LOCALIDADE", "AVULSO"]
        tipo_selecionado = st.radio("Tipo de Geração:", tipos_folha, horizontal=True)
        
        valor_selecionado = None
        arquivo_xlsx = None
        
        if tipo_selecionado in ["PT", "LOCALIDADE"]:
            coluna = tipo_selecionado
            # Modificado para obter contagens (Smart Select)
            valores_dict = db_manager.obter_valores_unicos_com_contagem(coluna)
            
            if valores_dict:
                # Criar lista formatada "VALOR (QTD)"
                opcoes_formatadas = [f"{v} ({q})" for v, q in valores_dict.items()]
                opcoes_formatadas.insert(0, "Selecione...")
                
                escolha = st.selectbox(f"Selecione o valor de **{coluna}** (registros disponíveis):", opcoes_formatadas)
                
                if escolha != "Selecione...":
                    # Extrair o valor real da string "VALOR (QTD)"
                    valor_selecionado = escolha.rsplit(' (', 1)[0]
                else:
                    valor_selecionado = None
            else:
                st.warning(f"Nenhum valor encontrado para {coluna} (ou todos já estão em 'prog')")
                
        elif tipo_selecionado == "AVULSO":
            st.markdown("""
            #### 📋 Importar Lista de CILs via Arquivo XLSX
            
            **Instruções:**
            1. Prepare um arquivo Excel (.xlsx) com uma coluna contendo os CILs
            2. A coluna preferencialmente deve se chamar **'cil'**
            3. Faça o upload do arquivo abaixo
            4. O sistema irá automaticamente detectar e extrair os CILs
            5. **Geração em Cadeia:** Após importar, você pode gerar múltiplas folhas sequencialmente até processar todos os CILs
            """)
            
            # Botão para resetar lista atual
            if 'avulso_cils_original' in st.session_state and st.session_state['avulso_cils_original']:
                col_reset1, col_reset2 = st.columns([3, 1])
                with col_reset1:
                    total_original = len(st.session_state['avulso_cils_original'])
                    processados = len(st.session_state.get('avulso_cils_processados', []))
                    restantes = total_original - processados
                    st.info(f"📊 **Lista Ativa:** {total_original} CILs | ✅ Processados: {processados} | ⏳ Restantes: {restantes}")
                with col_reset2:
                    if st.button("🔄 Nova Lista", help="Limpar lista atual e importar nova"):
                        st.session_state['avulso_cils_original'] = []
                        st.session_state['avulso_cils_processados'] = []
                        st.rerun()
            
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
                        
                    cils_do_arquivo = utils.extrair_cils_do_xlsx(arquivo_xlsx)
                    if cils_do_arquivo:
                        # Armazenar lista original na sessão
                        st.session_state['avulso_cils_original'] = cils_do_arquivo
                        if 'avulso_cils_processados' not in st.session_state:
                            st.session_state['avulso_cils_processados'] = []
                        
                        st.info(f"📊 {len(cils_do_arquivo)} CIL(s) único(s) identificado(s)")
                        st.write("**Primeiros CILs encontrados:**", ", ".join(cils_do_arquivo[:5]) + ("..." if len(cils_do_arquivo) > 5 else ""))
                except Exception as e:
                    st.error(f"❌ Erro ao processar arquivo: {e}")

        # --- Seleção de Critério (Apenas para PT/LOCALIDADE) ---
        criterio_selecionado = None
        valor_criterio_selecionado = None

        if tipo_selecionado != "AVULSO":
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
            st.info("ℹ️ **Modo Avulso:** A geração será baseada apenas na lista de CILs, ignorando critérios de seleção e estado atual.")
                
        # Parâmetros de Geração
        col1, col2 = st.columns(2)
        with col1:
            num_nibs_por_folha = st.number_input("NIBs por Folha:", min_value=1, value=50)
        with col2:
            max_folhas = st.number_input("Máximo de Folhas a Gerar:", min_value=1, value=10)

        # Botão de Simulação (Pré-visualização)
        if st.button("👁️ Simular / Pré-visualizar", type="primary"):
            if tipo_selecionado != "AVULSO" and not valor_selecionado:
                st.error("Por favor, selecione um valor válido de PT ou Localidade.")
            elif tipo_selecionado == "AVULSO" and not arquivo_xlsx and 'avulso_cils_original' not in st.session_state:
                st.error("Por favor, faça upload de um arquivo XLSX com a lista de CILs.")
            elif tipo_selecionado != "AVULSO" and (not criterio_selecionado or not valor_criterio_selecionado):
                st.error("Por favor, selecione um critério de filtro válido.")
            else:
                cils_validos = None
                if tipo_selecionado == "AVULSO":
                    # Usar lista armazenada ou extrair do arquivo
                    if 'avulso_cils_original' in st.session_state and st.session_state['avulso_cils_original']:
                        # Calcular CILs restantes (não processados)
                        cils_processados = set(st.session_state.get('avulso_cils_processados', []))
                        cils_originais = st.session_state['avulso_cils_original']
                        cils_validos = [cil for cil in cils_originais if cil not in cils_processados]
                        
                        if not cils_validos:
                            st.warning("⚠️ Todos os CILs da lista já foram processados! Use 'Nova Lista' para importar outra.")
                            st.stop()
                        else:
                            st.info(f"🔄 Usando {len(cils_validos)} CIL(s) restante(s) da lista armazenada.")
                    elif arquivo_xlsx:
                        cils_validos = utils.extrair_cils_do_xlsx(arquivo_xlsx)
                        if not cils_validos:
                            st.error("Nenhum CIL válido encontrado no arquivo XLSX.")
                            st.stop()
                    else:
                        st.error("Nenhuma lista de CILs disponível.")
                        st.stop()
                
                with st.spinner("Calculando pré-visualização..."):
                    preview = db_manager.simular_folhas_trabalho(
                        tipo_selecionado, valor_selecionado, 
                        max_folhas, num_nibs_por_folha, 
                        cils_validos, criterio_selecionado, valor_criterio_selecionado
                    )
                
                if preview and preview['total_registros'] > 0:
                    st.session_state['preview_data'] = preview
                    st.success("✅ Simulação concluída com sucesso!")
                else:
                    if 'preview_data' in st.session_state:
                         del st.session_state['preview_data']
                    st.warning("⚠️ Nenhum registro encontrado para os critérios selecionados.")
                    if preview and preview.get('cils_nao_encontrados'):
                         st.warning(f"CILs não encontrados: {len(preview['cils_nao_encontrados'])}")

        # Exibir Resultados da Simulação e Botão de Confirmação
        if 'preview_data' in st.session_state and st.session_state.get('preview_data', {}).get('total_registros', 0) > 0:
            p = st.session_state['preview_data']
            
            st.markdown("---")
            st.markdown("### 📊 Resultado da Simulação")
            
            col_p1, col_p2, col_p3, col_p4 = st.columns(4)
            col_p1.metric("Total Registros", p['total_registros'])
            col_p2.metric("NIBs Únicos", p['total_nibs'])
            col_p3.metric("Folhas Possíveis", p['folhas_possiveis'])
            col_p4.metric("Serão Geradas", p['folhas_a_gerar'])
            
            with st.expander("👀 Ver amostra dos dados (5 primeiros registros)"):
                st.dataframe(p['preview_df'])
                
            if p.get('cils_nao_encontrados'):
                with st.expander(f"⚠️ {len(p['cils_nao_encontrados'])} CILs não encontrados (AVULSO)"):
                    st.write(", ".join(p['cils_nao_encontrados']))

            st.markdown("---")
            if tipo_selecionado == "AVULSO":
                st.info("ℹ️ **Modo Avulso:** Os registros NÃO serão marcados como 'prog' no banco de dados.")
            else:
                st.warning("⚠️ **Atenção:** Ao confirmar, os registros serão marcados como 'prog' no banco de dados.")
            
            if st.button("🚀 Confirmar e Gerar Folhas Reais", type="primary"):
                 # Lógica original de geração
                 cils_validos_real = None
                 if tipo_selecionado == "AVULSO":
                    # Usar mesma lógica da simulação
                    if 'avulso_cils_original' in st.session_state and st.session_state['avulso_cils_original']:
                        cils_processados = set(st.session_state.get('avulso_cils_processados', []))
                        cils_originais = st.session_state['avulso_cils_original']
                        cils_validos_real = [cil for cil in cils_originais if cil not in cils_processados]
                    elif arquivo_xlsx:
                        cils_validos_real = utils.extrair_cils_do_xlsx(arquivo_xlsx)

                 with st.spinner("Gerando folhas de trabalho e ATUALIZANDO BANCO..."):
                    df_folhas, cils_nao_encontrados = db_manager.gerar_folhas_trabalho(
                        tipo_selecionado, 
                        valor_selecionado, 
                        max_folhas, 
                        num_nibs_por_folha, 
                        cils_validos_real,
                        criterio_selecionado,
                        valor_criterio_selecionado,
                        user_name=user['nome']
                    )
                    
                 # Limpar preview após gerar
                 if 'preview_data' in st.session_state:
                     del st.session_state['preview_data']

                 if df_folhas is not None and not df_folhas.empty:
                    st.success(f"✅ {df_folhas['FOLHA'].max()} Folhas geradas com sucesso.")
                    
                    # Marcar CILs como processados (apenas para AVULSO)
                    if tipo_selecionado == "AVULSO" and cils_validos_real:
                        cils_gerados = set(df_folhas['cil'].unique())
                        if 'avulso_cils_processados' not in st.session_state:
                            st.session_state['avulso_cils_processados'] = []
                        st.session_state['avulso_cils_processados'].extend(list(cils_gerados))
                        st.success(f"📝 {len(cils_gerados)} CIL(s) marcado(s) como processado(s) nesta geração.")
                    
                    zip_data = utils.generate_csv_zip(df_folhas, num_nibs_por_folha, criterio_selecionado, valor_criterio_selecionado)
                    nome_zip = f"Folhas_{criterio_selecionado}_{utils.sanitizar_nome_arquivo(valor_criterio_selecionado)}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
                    
                    st.download_button(
                        label="📦 Baixar Arquivo ZIP com Folhas (CSV)",
                        data=zip_data,
                        file_name=nome_zip,
                        mime="application/zip",
                        type="primary"
                    )
                    st.success("Download pronto! Clique acima.")
                 else:
                     st.error("Erro ao gerar folhas finais. Tente novamente.")

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
