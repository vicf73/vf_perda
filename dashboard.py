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

def mostrar_dashboard_geral(db_manager):
    """Dashboard geral com métricas e visualizações com seleção de critérios."""
    st.markdown("## 📊 Dashboard Geral - Métricas do Sistema")
    
    if not PLOTLY_AVAILABLE:
        st.error("""
        ❌ **Plotly não está disponível**
        
        Para visualizar os gráficos, instale o Plotly:
        ```bash
        pip install plotly
        ```
        """)
        return
    
    # --- SELEÇÃO DE CRITÉRIOS PARA DASHBOARD ---
    st.markdown("### 🔍 Seleção de Critérios para Análise")
    
    criterios_dashboard = [
        "Criterio", 
        "Anomalia", 
        "EST_CTR"
    ]
    
    col1, col2 = st.columns(2)
    
    with col1:
        criterio_principal = st.selectbox(
            "Critério Principal para Análise:",
            criterios_dashboard,
            index=0,
            help="Selecione o critério principal para os gráficos e análises"
        )
    
    with col2:
        # Filtro opcional por valor específico do critério
        valores_criterio = db_manager.obter_valores_unicos(criterio_principal.lower())
        filtro_valor = st.selectbox(
            f"Filtrar por valor específico de {criterio_principal}:",
            ["Todos"] + (valores_criterio if valores_criterio else []),
            help="Opcional: selecione um valor específico para filtrar os dados"
        )
    
    st.markdown("---")
    
    # Obter dados com base nos critérios selecionados
    with st.spinner("Carregando dados do dashboard..."):
        estatisticas = db_manager.obter_estatisticas_gerais()
        metricas = db_manager.obter_metricas_operacionais()
        
        # Obter dados específicos para o critério selecionado
        dados_criterio_selecionado = db_manager.obter_dados_para_dashboard(criterio_principal, filtro_valor if filtro_valor != "Todos" else None)
    
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
            value=f"{stats.get('total_valor', 0):,.2f} ECV",
            delta=None
        )
    
    st.markdown("---")
    
    # Gráficos e Visualizações baseados no critério selecionado
    col_left, col_right = st.columns(2)
    
    with col_left:
        # Gráfico de Distribuição pelo Critério Selecionado
        if dados_criterio_selecionado and 'distribuicao_criterio' in dados_criterio_selecionado:
            df_criterio = pd.DataFrame(dados_criterio_selecionado['distribuicao_criterio'])
            if not df_criterio.empty:
                try:
                    # Limitar a 15 itens para melhor visualização
                    df_criterio = df_criterio.head(15)
                    
                    fig_criterio = px.pie(
                        df_criterio, 
                        values='quantidade', 
                        names=criterio_principal.lower(),
                        title=f'Distribuição por {criterio_principal}',
                        hole=0.4
                    )
                    fig_criterio.update_layout(
                        showlegend=True,
                        legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.1)
                    )
                    st.plotly_chart(fig_criterio, use_container_width=True)
                except Exception as e:
                    st.error(f"Erro ao criar gráfico de {criterio_principal}: {e}")
                    # Fallback: mostrar tabela
                    st.dataframe(df_criterio, use_container_width=True)
            else:
                st.info(f"ℹ️ Sem dados de {criterio_principal} para exibir")
        else:
            st.info(f"ℹ️ Aguardando dados de {criterio_principal}")
    
    with col_right:
        # Gráfico de Barras com Valor Total por Critério
        if dados_criterio_selecionado and 'distribuicao_criterio' in dados_criterio_selecionado:
            df_criterio_valor = pd.DataFrame(dados_criterio_selecionado['distribuicao_criterio'])
            if not df_criterio_valor.empty:
                try:
                    # Ordenar por valor total e limitar a 10 itens
                    df_criterio_valor = df_criterio_valor.nlargest(10, 'total_valor')
                    
                    fig_barras = px.bar(
                        df_criterio_valor,
                        x=criterio_principal.lower(),
                        y='total_valor',
                        title=f'Top 10 {criterio_principal} por Valor Total',
                        color='total_valor',
                        labels={'total_valor': 'Valor Total (ECV)', criterio_principal.lower(): criterio_principal}
                    )
                    fig_barras.update_layout(
                        xaxis_tickangle=-45,
                        showlegend=False
                    )
                    st.plotly_chart(fig_barras, use_container_width=True)
                except Exception as e:
                    st.error(f"Erro ao criar gráfico de barras: {e}")
                    st.dataframe(df_criterio_valor, use_container_width=True)
            else:
                st.info(f"ℹ️ Sem dados de valor para {criterio_principal}")
    
    # --- ANÁLISE COMPARATIVA ENTRE CRITÉRIOS ---
    st.markdown("### 📈 Análise Comparativa")
    
    col_comp1, col_comp2 = st.columns(2)
    
    with col_comp1:
        # Selecionar segundo critério para comparação
        criterio_comparacao = st.selectbox(
            "Critério para Comparação:",
            [c for c in criterios_dashboard if c != criterio_principal],
            help="Selecione um segundo critério para análise comparativa"
        )
    
    with col_comp2:
        if st.button("🔄 Gerar Análise Comparativa", type="secondary"):
            with st.spinner("Gerando análise comparativa..."):
                dados_comparacao = db_manager.obter_dados_para_dashboard(criterio_comparacao, None)
                
                if dados_comparacao and 'distribuicao_criterio' in dados_comparacao:
                    df_comparacao = pd.DataFrame(dados_comparacao['distribuicao_criterio'])
                    if not df_comparacao.empty:
                        st.info(f"**Distribuição por {criterio_comparacao}**")
                        
                        # Gráfico de comparação
                        try:
                            df_comparacao_top = df_comparacao.nlargest(8, 'quantidade')
                            
                            fig_comparacao = px.bar(
                                df_comparacao_top,
                                x=criterio_comparacao.lower(),
                                y=['quantidade', 'total_valor'],
                                title=f'Comparação: {criterio_comparacao} (Quantidade vs Valor)',
                                barmode='group'
                            )
                            fig_comparacao.update_layout(xaxis_tickangle=-45)
                            st.plotly_chart(fig_comparacao, use_container_width=True)
                        except Exception as e:
                            st.error(f"Erro ao criar gráfico de comparação: {e}")
                            st.dataframe(df_comparacao[['quantidade', 'total_valor']].head(10), use_container_width=True)
    
    # --- ESTATÍSTICAS DETALHADAS DO CRITÉRIO SELECIONADO ---
    st.markdown(f"### 📋 Estatísticas Detalhadas - {criterio_principal}")
    
    if dados_criterio_selecionado and 'distribuicao_criterio' in dados_criterio_selecionado:
        df_detalhes = pd.DataFrame(dados_criterio_selecionado['distribuicao_criterio'])
        
        if not df_detalhes.empty:
            # Métricas resumidas
            total_registros_criterio = df_detalhes['quantidade'].sum()
            total_valor_criterio = df_detalhes['total_valor'].sum()
            valor_medio = total_valor_criterio / total_registros_criterio if total_registros_criterio > 0 else 0
            
            col_met1, col_met2, col_met3 = st.columns(3)
            
            with col_met1:
                st.metric(
                    f"Total Registros ({criterio_principal})",
                    f"{total_registros_criterio:,}"
                )
            
            with col_met2:
                st.metric(
                    f"Valor Total ({criterio_principal})",
                    f"{total_valor_criterio:,.2f} ECV"
                )
            
            with col_met3:
                st.metric(
                    f"Valor Médio ({criterio_principal})",
                    f"{valor_medio:,.2f} ECV"
                )
            
            # Tabela detalhada
            st.dataframe(
                df_detalhes.rename(columns={
                    criterio_principal.lower(): criterio_principal,
                    'quantidade': 'Quantidade',
                    'total_valor': 'Valor Total (ECV)'
                }),
                use_container_width=True,
                height=400
            )
            
            # Opção de download
            csv = df_detalhes.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📥 Download Dados Detalhados",
                data=csv,
                file_name=f"dashboard_{criterio_principal}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    # Mapa de Calor Geográfico (mantido da versão anterior)
    st.markdown("### 🗺️ Densidade Geográfica")
    if metricas.get('geolocalizacao'):
        df_geo = pd.DataFrame(metricas['geolocalizacao'])
        if not df_geo.empty and len(df_geo) > 1:
            try:
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
            except Exception as e:
                st.error(f"Erro ao criar mapa: {e}")
                st.info("📍 **Dados de localização disponíveis:**")
                st.dataframe(df_geo[['lat', 'long', 'densidade']].head(10), use_container_width=True)
        else:
            st.info("ℹ️ Dados geográficos insuficientes para exibir o mapa")
    else:
        st.info("ℹ️ Sem dados de geolocalização disponíveis")
