<h1 align="center">
PIPELINE - Tweets sobre COVID e saúde em português
</h1>

<p align="center">Demonstração de coleta, armazenamento e consumo de dados em Python com API do Twitter (#covid e #saúde) e banco de dados NoSQL Elasticsearch</p>
<p align="center">Publicado <a href="">aqui</a>.</p>

<p align="center">
  <a href="https://github.com/guipatriota/Pipeline_COVID_SAUDE_BR-HiTechnology/graphs/contributors">
    <img src="https://img.shields.io/github/contributors/guipatriota/Pipeline_COVID_SAUDE_BR-HiTechnology?color=%237159c1&logoColor=%237159c1&style=flat" alt="Contributors">
  </a>
  <a href="https://opensource.org/licenses/MIT">
    <img src="https://img.shields.io/github/license/guipatriota/Pipeline_COVID_SAUDE_BR-HiTechnology?color=%237159c1&logo=mit" alt="License">
  </a>
</p>

<hr>

Projeto para processo seletivo - Engenheiro de Dados - 26/07/2021

## Python + DJango* + Elasticsearch* + Prometheus* + Docker*

## Participantes

| [<img src="https://avatars3.githubusercontent.com/u/60905310?s=460&v=4" width="75px;"/>](https://github.com/guipatriota) |
| :------------------------------------------------------------------------------------------------------------------------: |


| [Guilherme D. Patriota](https://github.com/guipatriota)

___________________

# Introdução
Este projeto é uma demonstração de uso de técnicas de Engenharia de Dados para coleta, armazenamento e consumo de dados de fonte online livre.

Os dados são coletados pela API do Twitter com o uso dos seguintes filtros:
${"value": "COVID
lang:pt", "tag": "Covid
rule"},
{"value": "Saúde lang:pt",
"tag": "Saúde rule"}$


# a. Documentação do projeto
## Coleta dos dados
## API do Twitter
## Modelagem do banco de dados não relacional - Elasticsearch
## 
# b. Documentação das APIs
- Uso da API do Twitter
- Uso da API ElasticSearch
- Uso da API REST criada

# c. Sobre a arquitetura
# d. Como subir uma cópia em ambiente local
1. Clone este repositório em sua máquina local:

``git clone https://github.com/guipatriota/Pipeline_COVID_SAUDE_BR-HiTechnology.git``

2. Altere o arquivo .env da e inclua sua chave de autenticação bearer token
3. Monte a imagem docker e a execute