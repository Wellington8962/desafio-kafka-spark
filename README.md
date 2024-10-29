# Desafio Kafka e Spark: Simulação de Vendas E-commerce

Este projeto tem como objetivo simular um sistema de vendas em um e-commerce, onde utilizei o Apache Kafka para enviar e consumir dados em tempo real e o PySpark para processar essas mensagens. 

## Descrição do Desafio

### Etapas do Desafio

1. **Instalar o Apache Kafka**: Configurar o ambiente Kafka com Zookeeper e criar um tópico específico para as mensagens de vendas.

2. **Criar um Produtor de Mensagens em Python**: Desenvolver um produtor que envia mensagens com dados de vendas para o Kafka. Cada mensagem contém:
   - **ID da ordem**: Identificador único para cada venda.
   - **Documento do cliente**: CPF do cliente que realizou a compra.
   - **Produtos comprados**: Lista de produtos com nome e quantidade.
   - **Quantidade de cada produto**: Quantidade individual de cada item comprado.
   - **Valor total da venda**: Total em reais da compra.
   - **Data e hora da venda**: Data e hora da venda no formato `DD/MM/YYYY HH:MM:SS`.
   
   > **OBS**: Para a geração dos dados, foi utilizada a biblioteca `Faker`.

3. **Criar um Receptor de Mensagens em PySpark**: Desenvolver um consumidor em PySpark que lê as mensagens do Kafka, processa os dados e exibe o valor total das vendas agrupado por produto.

   - **Transformação**: A transformação aplicada foi o agrupamento das vendas por produto. O cálculo do valor total de vendas para cada produto é feito multiplicando a quantidade pelo preço unitário e, em seguida, somando os valores para cada item vendido.

---

## Estrutura do Projeto

- `producer_vendas_ecommerce.py`: Script Python que atua como produtor de mensagens, enviando dados de vendas simulados ao tópico Kafka.
- `consumer_pyspark_ecommerce.py`: Script PySpark que atua como consumidor, lê os dados de vendas do Kafka, aplica a transformação e exibe os resultados no console.
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

3. Instale as bibliotecas necessárias com:
   ```bash
   pip install -r requirements.txt

### 2. Iniciar o Apache Kafka

 > **OBS**: Certifique-se de que o Apache Kafka está instalado em sua máquina.

1. Inicie o Zookeeper:
   ```bash
   $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties

2. Em outro terminal, inicie o servidor Kafka:
   ```bash
   $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties

3. Crie o tópico `vendas_ecommerce`:
   ```bash
   $KAFKA_HOME/bin/kafka-topics.sh --create --topic vendas_ecommerce --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

### 3. Executar o Produtor de Mensagens e o Consumidor PySpark

1. Execute o script `producer_vendas_ecommerce.py` para gerar e enviar as mensagens de vendas simuladas ao Kafka:
   ```bash
   python3 producer_vendas_ecommerce.py
   
2. Execute o consumidor em PySpark com o conector Kafka para processar as mensagens:
   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 consumer_pyspark_ecommerce.py

> **Nota: Para interromper o processo de qualquer serviço do Kafka, como Zookeeper, Producer ou Consumer, pressione `Ctrl + C` no terminal. Isso encerra o serviço de forma segura caso seja necessário parar devido a um erro ou por qualquer outro motivo.**


## Conclusão

Este projeto demonstrou a integração do Apache Kafka e PySpark para processar dados de vendas em tempo real. Essa estrutura é útil para simular cenários de e-commerce e pode ser expandida para outras aplicações de monitoramento e análise em tempo real.

