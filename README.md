# Desafio Kafka e Spark: Simulação de Vendas E-commerce

Este projeto tem como objetivo simular um sistema de vendas em um e-commerce, onde utilizamos o Apache Kafka para enviar e consumir dados em tempo real e o PySpark para processar essas mensagens. 

## Descrição do Desafio

### Etapas do Desafio

1. **Instalar o Apache Kafka**: Configurar o ambiente Kafka com Zookeeper e criar um tópico específico para as mensagens de vendas.

2. **Criar um Produtor de Mensagens em Python**: Desenvolver um produtor que envia mensagens com dados de vendas para o Kafka. Cada mensagem contém:
   - **ID da ordem**: Identificador único para cada venda.
   - **Documento do cliente**: CPF do cliente que realizou a compra.
   - **Produtos comprados**: Lista de produtos com nome e quantidade.
   - **Quantidade de cada produto**: Quantidade individual de cada item comprado.
   - **Valor total da venda**: Total em reais da compra.
   - **Data e hora da venda**: Data e hora da venda em formato brasileiro `DD/MM/YYYY HH:MM:SS`.

   > **OBS**: Para a geração dos dados, foi utilizada a biblioteca `Faker`.

3. **Criar um Receptor de Mensagens em PySpark**: Desenvolver um consumidor em PySpark que lê as mensagens do Kafka, processa os dados e exibe o valor total das vendas agrupado por produto.

   - **Transformação**: A transformação aplicada foi o agrupamento das vendas por produto, somando o valor total vendido para cada item.

---

## Estrutura do Projeto

- `vendas_ecommerce.py`: Script Python que atua como produtor de mensagens, enviando dados de vendas simulados ao tópico Kafka.
- `pyspark_consumer_vendas.py`: Script PySpark que atua como consumidor, lê os dados de vendas do Kafka, aplica a transformação e exibe os resultados no console.
- `requirements.txt`: Arquivo com as dependências do projeto para fácil replicação do ambiente.
- `README.md`: Instruções e detalhes do projeto.

---

## Pré-requisitos

- Python 3.10
- Apache Kafka e Zookeeper instalados e configurados
- Ambiente virtual configurado com as dependências do projeto

## Instruções de Execução

### 1. Configuração do Ambiente

1. Clone este repositório.
2. Crie e ative um ambiente virtual:
   ```bash
   python3.10 -m venv venv
   source venv/bin/activate
