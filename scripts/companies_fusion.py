from data_processing import DataProcessor

# Paths
PATH_RAW_JSON = 'data_raw/dados_empresaA.json'
PATH_RAW_CSV = 'data_raw/dados_empresaB.csv'
PATH_PROCESSED_CSV = 'data_processed/data_transformed.csv'

# Transformação de chaves
COLUMN_MAPPING = {
    'Nome do Item': 'Nome do Produto',
    'Classificação do Produto': 'Categoria do Produto',
    'Valor em Reais (R$)': 'Preço do Produto (R$)',
    'Quantidade em Estoque': 'Quantidade em Estoque',
    'Nome da Loja': 'Filial',
    'Data da Venda': 'Data da Venda'
}

def main():
    # Extract
    company_a_data = DataProcessor(PATH_RAW_JSON, 'json')
    company_b_data = DataProcessor(PATH_RAW_CSV, 'csv')

    # Transform
    company_b_data.rename_columns(COLUMN_MAPPING)
    combined_data = DataProcessor.join(company_a_data, company_b_data)

    # Load
    combined_data.save_to_csv(PATH_PROCESSED_CSV)
    print(f"Dados salvos em {PATH_PROCESSED_CSV}")

if __name__ == '__main__':
    main()
