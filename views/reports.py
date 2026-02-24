# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import datetime

# Tentar importar Plotly com fallback
try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

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
            col1.metric("Total do Relatório", f"{total_valor:,.2f} ECV")
            col2.metric("Valor Médio", f"{media_valor:,.2f} ECV")
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
    
    if not PLOTLY_AVAILABLE:
        st.error("Plotly necessário para visualizações gráficas. Instale com: pip install plotly")
        # Mostrar apenas tabelas
        with st.spinner("Carregando métricas de eficiência..."):
            metricas = db_manager.obter_metricas_operacionais()
        
        if metricas.get('eficiencia_pt'):
            df_eficiencia = pd.DataFrame(metricas['eficiencia_pt'])
            st.dataframe(df_eficiencia, use_container_width=True)
        return
    
    with st.spinner("Carregando métricas de eficiência..."):
        metricas = db_manager.obter_metricas_operacionais()
    
    if not metricas.get('eficiencia_pt'):
        st.info("ℹ️ Sem dados de eficiência disponíveis")
        return
    
    df_eficiencia = pd.DataFrame(metricas['eficiencia_pt'])
    
    # Gráfico de eficiência
    try:
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
    except Exception as e:
        st.error(f"Erro ao criar gráfico de eficiência: {e}")
    
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
        
        try:
            fig_localidades = px.treemap(
                df_localidades.head(8),
                path=['localidade'],
                values='valor_total',
                title='Distribuição de Valor por Localidade (Top 8)'
            )
            st.plotly_chart(fig_localidades, use_container_width=True)
        except Exception as e:
            st.error(f"Erro ao criar treemap: {e}")
            st.dataframe(df_localidades, use_container_width=True)

def mostrar_relatorio_usuarios(db_manager):
    """Relatório de atividade de usuários."""
    st.markdown("## 👥 Relatório de Usuários")
    
    if not PLOTLY_AVAILABLE:
        st.warning("Gráficos de usuários não disponíveis sem Plotly")
    
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
            if PLOTLY_AVAILABLE:
                try:
                    role_count = df_usuarios['Role'].value_counts()
                    fig_roles = px.pie(
                        values=role_count.values,
                        names=role_count.index,
                        title='Distribuição de Usuários por Função'
                    )
                    st.plotly_chart(fig_roles, use_container_width=True)
                except Exception as e:
                    st.error(f"Erro ao criar gráfico de roles: {e}")
            
            # Tabela de usuários
            st.markdown("### 📋 Lista de Usuários")
            st.dataframe(df_usuarios, use_container_width=True)
            
        else:
            st.info("ℹ️ Nenhum usuário cadastrado no sistema")
            
    except Exception as e:
        st.error(f"❌ Erro ao carregar relatório de usuários: {e}")
