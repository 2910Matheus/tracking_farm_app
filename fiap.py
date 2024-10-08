"""
Módulo de Rastreamento de Produtos Agrícolas
Este módulo contém funções para cadastro, consulta e validação de produtos e veículos
em um sistema de transporte agrícola. Ele permite o registro de informações sobre produtos,
como quantidade, origem, destino e necessidades de transporte (ventilação e proteção solar),
bem como a comparação de veículos disponíveis para transporte.
"""
import json
import oracledb
import pandas as pd
import os

def valor_nulo(campo, nome_campo):
    """
    Verifica se o campo está vazio (nulo).

    Args:
        campo (str): Valor do campo a ser verificado.
        nome_campo (str): Nome do campo para exibir na mensagem de erro.

    Returns:
        bool: Retorna True se o campo estiver vazio, caso contrário False.
    """
    if not campo:
        print(f"O {nome_campo} não pode ser vazio!")
        return True
    return False 
    
def validar_positivo(numero, nome_campo):
    """
    Valida se o número fornecido é positivo.

    Args:
        numero (int, float): Número a ser validado.
        nome_campo (str): Nome do campo para exibir na mensagem de erro.

    Returns:
        bool: Retorna True se o número for inválido (não positivo), caso contrário False.
    """
    try:
        if numero <= 0:
            raise ValueError(f"O valor de {nome_campo} deve ser positivo.")
    except ValueError as e:
        print("Digite um número válido!")
        return True
    return False

def perguntar_sim_nao(pergunta):
    """
    Faz uma pergunta ao usuário esperando uma resposta de 'sim' ou 'não'.

    Args:
        pergunta (str): Pergunta a ser feita ao usuário.

    Returns:
        bool: Retorna True se a resposta for 'sim', caso contrário False.
    """
    while True:
        resposta = input(pergunta).lower()
        if resposta in ['sim', 'não', 'nao']:
            return resposta == 'sim'
        else:
            print("Por favor, responda apenas com 'sim' ou 'não'.")

def dados_coletados_produtos():
    """
    Coleta dados sobre os produtos para transporte agrícola e insere no banco de dados.

    Returns:
        dict: Dicionário contendo os dados do produto coletado.
    """
    while True:
        try:
            produto = input('Informe o nome do produto: ')
            if valor_nulo(produto, "produto"):
                continue
            origem = input('Informe a origem do produto: ')
            if valor_nulo(origem, "origem"):
                continue
            destino = input('Informe o destino do produto: ')
            if valor_nulo(destino, "destino"):
                continue
            quantidade = int(input('Informe a quantidade do produto: '))
            if validar_positivo(quantidade, "quantidade"):
                continue
            temperatura_minima = float(input('Informe a temperatura mínima recomendada: '))
            temperatura_maxima = float(input('Informe a temperatura máxima recomendada: '))
            ventilacao = perguntar_sim_nao('Requer ventilação (sim/não): ')
            protecao_solar = perguntar_sim_nao('Requer proteção contra luz solar (sim/não): ')
        except ValueError:
            print("Digite um número válido!")
        else:
            break

    dados_produtos = {
        'produto': produto,
        'quantidade': quantidade,
        'origem': origem,
        'destino': destino,
        'temperatura_minima': temperatura_minima,
        'temperatura_maxima': temperatura_maxima,
        'ventilacao': 'SIM' if ventilacao else 'NAO',  # Converte True/False para 'SIM' ou 'NAO'
        'protecao_solar': 'SIM' if protecao_solar else 'NAO'  # Converte True/False para 'SIM' ou 'NAO'
    }

    try:
        inst_cadastro.execute("""
            INSERT INTO transporte_agricola_produtos (produto, quantidade, origem, destino, temperatura_minima, temperatura_maxima, ventilacao, protecao_solar)
            VALUES (:produto, :quantidade, :origem, :destino, :temperatura_minima, :temperatura_maxima, :ventilacao, :protecao_solar)
        """, dados_produtos)
        conn.commit()
        
        print("\n##### Dados GRAVADOS no banco #####")
    except oracledb.DatabaseError as e:
        error, = e.args
        print(f"Erro ao inserir dados: {error.message}")

    return dados_produtos

def dados_coletados_transporte():
    """
    Coleta dados sobre os veículos de transporte e insere no banco de dados.

    Returns:
        dict: Dicionário contendo os dados do transporte coletado.
    """
    while True:
        try:
            capacidade = int(input('Informe a capacidade do transporte: '))
            temperatura_transporte = float(input('Informe a temperatura do transporte: '))
            ventilacao = perguntar_sim_nao('O transporte tem sistema de ventilação (sim/não): ')
            protecao_solar = perguntar_sim_nao('O transporte tem proteção contra luz solar (sim/não): ')

        except ValueError:
            print("Digite um valor válido!")
        else:
            break

    dados_transporte = {
        'capacidade': capacidade,
        'temperatura': temperatura_transporte,
        'ventilacao': 'SIM' if ventilacao else 'NAO',
        'protecao_solar': 'SIM' if protecao_solar else 'NAO'
    }

    try:
        inst_cadastro.execute("""
            INSERT INTO transporte_agricola_veiculos (capacidade, temperatura, ventilacao, protecao_solar)
            VALUES (:capacidade, :temperatura, :ventilacao, :protecao_solar)
        """, dados_transporte)
        conn.commit()
        
        print("\n##### Dados GRAVADOS no banco #####")
    except oracledb.DatabaseError as e:
        error, = e.args
        print(f"Erro ao inserir dados: {error.message}")

    return dados_transporte

def consultar_produtos():
    """
    Consulta os produtos cadastrados no banco de dados.

    Returns:
        list: Lista de produtos cadastrados.
    """
    inst_consulta.execute('SELECT * FROM transporte_agricola_produtos')
    data = inst_consulta.fetchall()
    return data

def listar_produtos():
    """
    Lista e exibe todos os produtos cadastrados no banco de dados.

    Returns:
        list: Lista de produtos cadastrados.
    """
    print("----- LISTAR PRODUTOS -----\n")
    produtos = consultar_produtos() 
    if produtos is None or not produtos:
        print("Não há produtos cadastrados.")
        return None

    produtos_df = pd.DataFrame.from_records(produtos, columns=['id_produto', 'produto', 'quantidade', 'origem', 'destino', 'temperatura_minima', 'temperatura_maxima', 'ventilacao', 'protecao_solar'], index='id_produto')
    print(produtos_df)
    print("\n##### LISTADOS! #####")
    
    return produtos

def comparar_veiculo():
    """
    Compara a compatibilidade entre um veículo e o produto para transporte, verificando as condições de capacidade, temperatura, ventilação e proteção solar.

    Returns:
        bool: Retorna True se um veículo compatível for encontrado, caso contrário False.
    """
    veiculos = consultar_veiculos()
    if veiculos is None:
        os.system('cls')
        print('Não há veículos para comparação.')
        return 

    veiculo_df = pd.DataFrame.from_records(veiculos, columns=['id_veiculo', 'capacidade', 'temperatura', 'ventilacao', 'protecao_solar'], index='id_veiculo')
    
    produtos = listar_produtos()
    if produtos is None:
        print("Não há produtos cadastrados.")
        return
    id_produto = input('Insira o ID do produto: ')
    if produtos is not None:
        produtos_df = pd.DataFrame.from_records(produtos, columns=['id_produto', 'produto', 'quantidade', 'origem', 'destino', 'temperatura_minima', 'temperatura_maxima', 'ventilacao', 'protecao_solar'], index='id_produto')

        if id_produto.isdigit() and int(id_produto) in produtos_df.index:
            produto_quantidade = produtos_df.loc[int(id_produto), 'quantidade']  
            temperatura_minima = produtos_df.loc[int(id_produto), 'temperatura_minima']
            temperatura_maxima = produtos_df.loc[int(id_produto), 'temperatura_maxima']
            ventilacao_produto = produtos_df.loc[int(id_produto), 'ventilacao']
            protecao_solar_produto = produtos_df.loc[int(id_produto), 'protecao_solar']

            for idx, veiculo in veiculo_df.iterrows():
                capacidade_ok = produto_quantidade <= veiculo['capacidade']
                temperatura_ok = temperatura_minima <= veiculo['temperatura'] <= temperatura_maxima
                ventilacao_ok = (ventilacao_produto == 'SIM' and veiculo['ventilacao'] == 'SIM') or (ventilacao_produto == 'NAO' and veiculo['ventilacao'] == 'NAO')
                protecao_solar_ok = (protecao_solar_produto == 'SIM' and veiculo['protecao_solar'] == 'SIM') or (protecao_solar_produto == 'NAO' and veiculo['protecao_solar'] == 'NAO')

                if capacidade_ok and temperatura_ok and ventilacao_ok and protecao_solar_ok:
                    print(f"Veículo {idx} disponível com todas as condições atendidas:")
                    print(f"  - Capacidade: {veiculo['capacidade']} (produto: {produto_quantidade})")
                    print(f"  - Temperatura: {veiculo['temperatura']} (mínima: {temperatura_minima}, máxima: {temperatura_maxima})")
                    print(f"  - Ventilação: {veiculo['ventilacao']}")
                    print(f"  - Proteção Solar: {veiculo['protecao_solar']}")
                    return True
            
            print(f"\n### Erro: Nenhum veículo disponível atende todas as condições para o produto {produtos_df.loc[int(id_produto), 'produto']}. ###")
            print('\n')
            listar_veiculos()
            return False
        else:
            os.system('cls')
            print(f"Produto com ID {id_produto} não encontrado.")
            return comparar_veiculo()
    else:
        print("Não há produtos cadastrados.")
        return None

def consultar_veiculos():
    """
    Consulta os veículos cadastrados no banco de dados.

    Returns:
        list: Lista de veículos cadastrados.
    """
    inst_consulta.execute('SELECT * FROM transporte_agricola_veiculos')
    data = inst_consulta.fetchall()
    return data

def listar_veiculos():
    """
    Lista e exibe todos os veículos cadastrados no banco de dados.

    Returns:
        list: Lista de veículos cadastrados.
    """
    print("----- LISTAR VEICULOS -----\n")
    veiculos = consultar_veiculos()
    if veiculos is None or not veiculos:
        print("Não há veiculos cadastrados.")
        return None
    veiculos_df = pd.DataFrame.from_records(veiculos, columns=['id_veiculo', 'capacidade', 'temperatura', 'ventilacao', 'protecao_solar'], index='id_veiculo')
    print(veiculos_df)
    return veiculos_df

def menu():
    """
    Exibe o menu principal do sistema e aguarda uma escolha do usuário.

    Exibe opções para cadastro de produtos, veículos, listagem, verificação de compatibilidade e salvar 
    em JSON. O usuário deve escolher uma opção válida.

    Returns:
        int: Retorna o número da opção escolhida.
    """
    margem = ' ' * 12 # Define uma margem para a exibição da aplicação
    while True:
        try:
            print("""
            [1] - Cadastrar entrega
            [2] - Cadastrar veiculo
            [3] - Listar produtos
            [4] - Listar veiculos
            [5] - Verificar compatibilidade para entrega
            [6] - Salvar em JSON
            [7] - Sair

        """)
            escolha_menu = int(input(margem + 'Escolha: '))
            if escolha_menu not in [1, 2, 3, 4, 5,6,7]:
                os.system('cls')
                raise ValueError("Escolha inválida. Tente novamente.")
            return escolha_menu
        except ValueError as e:
            os.system('cls')
            print("Escolha inválida. Tente novamente.")
        
def sair():
    """
    Finaliza a execução do programa.

    Fecha a conexão com o banco de dados (se estiver aberta) e encerra a aplicação.
    """
    os.system('cls')
    print("Saindo do programa...")
    # Fecha a conexão com o banco de dados se estiver aberta
    if conn is not None:
        conn.close()  
    exit(0)

def opcao_escolhida(escolha):
    """
    Executa a função correspondente à opção selecionada pelo usuário no menu.

    Com base na escolha feita pelo usuário, a função correspondente é chamada para realizar a ação desejada.
    
    Args:
        escolha (int): Número da opção escolhida pelo usuário no menu.

    Raises:
        ValueError: Se a escolha não for um número válido dentro do intervalo das opções.
    """
    while True:
        match escolha:
            case 1:
                os.system('cls')
                dados_coletados_produtos()
            case 2:
                os.system('cls')
                dados_coletados_transporte()
            case 3: 
                os.system('cls')
                listar_produtos()
            case 4:
                os.system('cls')
                listar_veiculos()
            case 5:
                os.system('cls')
                comparar_veiculo()
            case 6:
                opcao_seis()
            case 7:
                os.system('cls')
                sair()
                
        
        input("Pressione ENTER")
        os.system('cls')
        escolha = menu()

def opcao_seis():
    """
    Executa a opção seis do menu, responsável por salvar os dados de produtos e veículos em arquivos JSON.

    Essa função chama `salvar_json()` para permitir que o usuário escolha quais dados deseja salvar
    em formato JSON (produtos, veículos ou ambos).
    """
    try:
        os.system('cls')
        opcao_seis = int(input("""
        Escolha:
        [1] - Salvar Produtos.
        [2] - Salvar Veiculos.
        """))
        if opcao_seis == 1:
            salvar_dados_produtos_json()
        if opcao_seis == 2:
            salvar_veiculo_json()
    except ValueError as e:
        os.system('cls')
        print("Escolha inválida. Tente novamente.")

def salvar_dados_produtos_json():
    """
    Salva os dados dos produtos cadastrados no banco de dados em um arquivo JSON.

    O arquivo JSON é gerado com as informações dos produtos cadastrados, incluindo 
    nome, quantidade, temperatura mínima e máxima recomendadas.

    Returns:
        str: Caminho do arquivo JSON gerado.
    """
    produtos = listar_produtos()
    if produtos:
        id_produto = input('Insira o ID do produto que deseja salvar em JSON: ')
        if id_produto.isdigit() and int(id_produto) in [p[0] for p in produtos]:
            produto_escolhido = [p for p in produtos if p[0] == int(id_produto)][0]
            dados_produto = {
                'id_produto': produto_escolhido[0],
                'produto': produto_escolhido[1],
                'quantidade': produto_escolhido[2],
                'origem': produto_escolhido[3],
                'destino': produto_escolhido[4],
                'temperatura_minima': produto_escolhido[5],
                'temperatura_maxima': produto_escolhido[6],
                'ventilacao': produto_escolhido[7],
                'protecao_solar': produto_escolhido[8],
            }
            arquivo = f"produto_{dados_produto['produto']}.json"
            with open(arquivo, 'w') as json_file:
                json.dump(dados_produto, json_file, indent=4)
            print(f"Dados do produto {dados_produto['produto']} salvos no arquivo {arquivo}.")
        else:
            print("ID do produto inválido.")
    else:
        print("Não há produtos para salvar em JSON.")

def salvar_veiculo_json():
    """
    Salva os dados dos veículos cadastrados no banco de dados em um arquivo JSON.

    O arquivo JSON é gerado com as informações atuais dos veículos cadastrados,
    incluindo capacidade, temperatura mínima e máxima, ventilação e proteção solar.

    Returns:
        str: Caminho do arquivo JSON gerado.
    """
    veiculos = listar_veiculos()

    if veiculos is not None and not veiculos.empty:
        id_veiculo = input('Insira o ID do veículo que deseja salvar em JSON: ')

        if id_veiculo.isdigit() and int(id_veiculo) in veiculos.index:
            veiculo_escolhido = veiculos.loc[int(id_veiculo)]

            dados_veiculo = {
                'id_veiculo': int(id_veiculo),
                'capacidade': veiculo_escolhido['capacidade'],
                'temperatura': veiculo_escolhido['temperatura'],
                'ventilacao': veiculo_escolhido['ventilacao'],
                'protecao_solar': veiculo_escolhido['protecao_solar'],
            }

            arquivo = f"veiculo_{dados_veiculo['id_veiculo']}.json"
            with open(arquivo, 'w') as json_file:
                json.dump(dados_veiculo, json_file, indent=4)
            print(f"Dados do veículo {dados_veiculo['id_veiculo']} salvos no arquivo {arquivo}.")
        else:
            print("ID do veículo inválido.")
    else:
        print("Não há veículos para salvar em JSON.")

import oracledb

#Necessita informar dados de acesso ao banco:
def conectar_banco_oracle():
    """
    Estabelece uma conexão com o banco de dados Oracle.

    Usa as credenciais e parâmetros de conexão configurados para conectar ao banco de dados Oracle,
    permitindo a execução de consultas e operações de manipulação de dados.
    
    Returns:
        connection (cx_Oracle.Connection): Objeto de conexão estabelecido com o banco de dados Oracle.

    Raises:
        cx_Oracle.DatabaseError: Se ocorrer algum erro ao tentar estabelecer a conexão.
    """
    try:
        conn = oracledb.connect(user='rmxxxx', password='DDMMAA', dsn='oracle.fiap.com.br:1521/ORCL') #Informar credencias
        return conn 
    except oracledb.DatabaseError as e:
        print("Erro ao conectar ao banco de dados:", e)
        return None 


if __name__ == "__main__":
    conn = conectar_banco_oracle()

    if conn:
        inst_cadastro = conn.cursor()
        inst_consulta = conn.cursor()
        inst_alteracao = conn.cursor()
        inst_exclusao = conn.cursor()
        escolha = menu()
        opcao_escolhida(escolha)
        dados_produtos = dados_coletados_produtos()
        inst_cadastro.close()
        inst_consulta.close()
        inst_alteracao.close()
        inst_exclusao.close()
        conn.close()
    else:
        print('Não foi possível estabelecer conexão com o banco de dados, tente novamente.')
