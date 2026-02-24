# -*- coding: utf-8 -*-
import re
import io
from io import BytesIO
from zipfile import ZipFile
import pandas as pd
import chardet
import streamlit as st
import logging

logger = logging.getLogger(__name__)

def sanitizar_nome_arquivo(nome):
    """Remove caracteres inválidos para nomes de arquivo."""
    if not nome:
        return "arquivo"
    # Remove caracteres inválidos e substitui espaços por underscore
    nome_seguro = re.sub(r'[<>:"/\\|?*]', '', nome)
    nome_seguro = nome_seguro.replace(' ', '_')
    # Limita o tamanho do nome para evitar problemas com paths longos
    return nome_seguro[:100]

def generate_csv_zip(df_completo, num_nibs_por_folha, criterio_tipo, criterio_valor):
    """Gera um arquivo ZIP contendo múltiplas folhas CSV com apenas as 10 primeiras colunas."""
    
    max_folha = df_completo['FOLHA'].max()
    
    # Define as 10 primeiras colunas que serão exportadas
    colunas_exportar = [
        'cil', 'prod', 'contador', 'leitura', 'mat_contador',
        'med_fat', 'qtd', 'valor', 'situacao', 'acordo'
    ]
    
    # Verifica se todas as colunas existem no DataFrame
    colunas_disponiveis = [col for col in colunas_exportar if col in df_completo.columns]
    
    if len(colunas_disponiveis) < len(colunas_exportar):
        st.warning(f"⚠️ Algumas colunas não encontradas. Exportando {len(colunas_disponiveis)} colunas.")
    
    # Sanitiza o nome do critério
    criterio_nome_seguro = sanitizar_nome_arquivo(criterio_valor)
    
    # Cria um buffer de memória para o ZIP
    zip_buffer = BytesIO()
    
    with ZipFile(zip_buffer, 'w') as zip_file:
        for i in range(1, max_folha + 1):
            folha_df = df_completo[df_completo['FOLHA'] == i]
            
            # Seleciona apenas as colunas desejadas
            folha_df_export = folha_df[colunas_disponiveis].copy()
            
            # Cria um buffer de memória para o arquivo CSV
            csv_buffer = BytesIO()
            
            # Exporta para CSV
            folha_df_export.to_csv(csv_buffer, index=False, encoding='utf-8-sig', sep=';')
            csv_buffer.seek(0)
            
            # Nome do arquivo personalizado com o critério
            nome_arquivo = f'{criterio_tipo}_{criterio_nome_seguro}_Folha_{i}.csv'
            
            # Adiciona o arquivo CSV ao ZIP
            zip_file.writestr(nome_arquivo, csv_buffer.getvalue())

    zip_buffer.seek(0)
    return zip_buffer.read()

def extrair_cils_do_xlsx(arquivo_xlsx):
    """Extrai a lista de CILs de um arquivo XLSX com diferentes formatos."""
    try:
        # Lê o arquivo XLSX
        df = pd.read_excel(arquivo_xlsx)
        
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
        cils = cils[~cils.str.lower().isin(['cil', 'cils', 'código', 'codigo', 'nome', 'numero', 'número', ''])]
        
        # Converte para lista e remove duplicatas
        cils_unicos = list(set(cils.tolist()))
        
        # Filtra apenas valores não vazios e válidos
        cils_validos = [cil for cil in cils_unicos if cil and cil != 'nan' and cil.strip()]
        
        st.success(f"📊 {len(cils_validos)} CIL(s) único(s) extraído(s)")
        logger.info(f"CILs extraídos do XLSX: {len(cils_validos)} válidos")
        
        return cils_validos
        
    except Exception as e:
        error_msg = f"❌ Erro ao ler arquivo XLSX: {str(e)}"
        st.error(error_msg)
        logger.error(error_msg)
        return []

def detectar_encoding(arquivo_csv):
    """Detecta o encoding do arquivo."""
    raw_data = arquivo_csv.getvalue()
    result = chardet.detect(raw_data)
    encoding = result['encoding'] or 'utf-8'
    logger.info(f"Encoding detectado: {encoding} (confiança: {result['confidence']})")
    return encoding

def detectar_separador(arquivo_csv, encoding):
    """Detecta o separador mais provável (',' ou ';')."""
    arquivo_csv.seek(0)
    try:
        amostra = arquivo_csv.read(1024 * 50).decode(encoding, errors='ignore')
        
        virgula_count = amostra.count(',')
        ponto_virgula_count = amostra.count(';')
        
        if ponto_virgula_count > virgula_count * 2:
            separador = ';'
        else:
            separador = ','
            
        logger.info(f"Separador detectado: '{separador}' (;: {ponto_virgula_count}, ,: {virgula_count})")
        return separador
    finally:
        arquivo_csv.seek(0)

def safe_streamlit_call(func):
    """Decorator para prevenir erros de renderização no Streamlit"""
    import functools
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if "removeChild" in str(e) or "Node" in str(e):
                logger.warning(f"Erro de renderização ignorado: {e}")
                return None
            else:
                raise e
    return wrapper

def clean_session_state():
    """Limpa estados temporários da sessão para prevenir conflitos"""
    keys_to_keep = ['authenticated', 'user', 'page_loaded', 'last_refresh']
    keys_to_remove = [key for key in st.session_state.keys() if key not in keys_to_keep]
    
    for key in keys_to_remove:
        try:
            del st.session_state[key]
        except:
            pass
