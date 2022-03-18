#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  3 23:53:55 2021

@author: gustavo
"""
# Library imports

import pandas as pd
import numpy as np
import math
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from datetime import datetime
import re
import sqlite3
from sqlalchemy import create_engine
import logging
import os


# Functions

def data_collection (url):
    
    
# ====================== Data collection ========================================


    # Script final para a coleta 
    
    
    
    # Coleta de url's de todas as categorias:
    
    # Instanciando a requisição
    
    url = 'https://books.toscrape.com/' #url
    
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'}) # requisição
    
    
    html_page = urlopen(req).read()
    
    html_page
    
    
    # Instanciando o BeautifulSoup
    
    soup = BeautifulSoup(html_page, 'html.parser')
    
    
    
    # Loop for para coleta dos links de todas as categorias de livros em uma lista
    
    urls = []
    
    for i in range(1, len(soup.find_all(href=re.compile("(catalogue[\/]category[\/].+)")))):
        
        lista_temporaria = str(re.search('href="(catalogue[\/]category[\/].+)"', str(soup.find_all(href=re.compile("(catalogue[\/]category[\/].+)"))[i])).group(1))
        
        urls.append(lista_temporaria)
    
    
    
    
    
    
    df_raw = pd.DataFrame(columns=['title', 'price', 'number of reviews', 'availability', 'url',
           'catalogue', 'time of scrapping'])
    
    
    
    for i in urls:
        
    
        # Instanciando a requisição
    
        url = 'https://books.toscrape.com/'+i #url
        logger.debug('Getting book categories information %s', url)
    
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'}) # requisição
    
    
        html_page = urlopen(req).read()
    
        html_page
    
    
        # Instanciando o BeautifulSoup
    
        soup = BeautifulSoup(html_page, 'html.parser')
    
        soup
    
        # Extraindo a classificação do livro
    
        catalogue = soup.find_all('h1')[0].get_text()
    
        catalogue
    
        # Extraindo o nome dos livros da vitrine
    
        lista_book_names = [i['alt'] for i in soup.find_all( class_='thumbnail')]
    
        lista_book_names 
    
        # Usando o nome dos livros para obter as url's da página de detalhes de cada produto
    
        url_books = [list(filter(None, soup.find_all( 'a', title=i)[0]['href'].split('../')))[0] for i in lista_book_names]
    
        url_books
    
    
    
        
    
        # Loop for para navegação e coleta de detalhes de cada livro dentro da seção 
    
    
        lista_price = []
        lista_avaliacao = []
        lista_disponibilidade = []
        lista_star = []
    
        for it in url_books:
    
            # Instanciando a requisição
    
            url = 'https://books.toscrape.com/catalogue/'+it #url
            logger.debug('Getting book details %s', url)
    
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'}) # requisição
    
    
            html_page = urlopen(req).read()
    
    
            # Instanciando o BeautifulSoup
    
            soup = BeautifulSoup(html_page, 'html.parser')
    
    
    
            # Extraindo o preço
    
            lista_price.append(soup.find_all(class_='price_color')[0].get_text()) 
    
            # Extraindo avaliação dos consumidores
    
            lista_avaliacao.append(soup.find_all('td')[-1].get_text())
            
            # Extraindo as estrelas
            lista_star.append(re.search('[A-Z]{1}[a-z]+', str(soup.find_all('p')[2])).group(0))
    
            # Extraindo a disponibilidade do produto
    
            lista_disponibilidade.append(soup.find_all('td')[-2].get_text())
    
    
    
        # Montar o dataframe com preço, avaliação dos consumidores e disponibilidade
    
        df_description = pd.DataFrame(data=[lista_book_names, lista_price, lista_avaliacao, lista_star, lista_disponibilidade, url_books],
                               ).transpose()
    
        df_description
    
        # Ajustando o nome das colunas
    
        df_description.columns = ['title', 'price', 'number of reviews' , 'star', 'availability', 'url']
    
        df_description
    
        # Ajustando a coluna 'url'
    
        df_description['url'] = ['https://books.toscrape.com/catalogue/'+i for i in df_description['url']]
    
        df_description
    
        # Inserindo a coluna 'Classificação'
    
        df_description['catalogue'] = catalogue
    
        df_description
    
        # Inserindo o horário da coleta
    
        df_description['time of scrapping'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    
        # Populando o df_raw
    
        df_raw = pd.concat([df_raw, df_description])
        
        
        # Atualizando o número das linhas
        
        df_raw.index = range(len(df_raw))
        
        # Atualizando a ordem das colunas
        
        df_raw = df_raw[['title', 'catalogue', 'number of reviews',
                         'star', 'price', 'availability', 'url', 'time of scrapping']]
        
    return df_raw


def data_transformation(df_raw):
    
    
    # ================================ Data cleaning ===================================================


    # Loading data
    
    df = df_raw.copy()
    
    
    # Dropping first column
    
    df = df.drop(columns=['number of reviews'])
    
    
    # Removing '£' simbol using regex
    
    df['price'] = df['price'].apply(lambda x: re.search('[0-9]{1,}.[0-9]{2}', x).group(0))
    
    
    # Removing 'In stock (' and 'available)' from column availability and keeping the number
    
    df['availability'] = df['availability'].apply(lambda x: re.search('[0-9]{1,}', x).group(0))
    
    
    # Replacing numerals with numbers in 'star' column
    
    df['star'] = df['star'].apply(lambda x: x.replace('One', '1') if x=='One' 
                     else x.replace('Two', '2') if x=='Two' 
                     else x.replace('Three', '3') if x=='Three'
                     else x.replace('Four', '4') if x=='Four' 
                                  else x.replace('Five', '5'))
    
    
    
    # Colocando o nome das colunas em caixa baixa e substituindo espaços vazios por underscore
    
    df.columns = [i.lower().replace(' ', '_') for i in df.columns]
    
    
    # Setting data types
    
    df['price'] = df['price'].astype(float)
    
    df[['star', 'availability']] = df[['star', 'availability']].astype(int)
    
    df['time_of_scrapping'] = pd.to_datetime(df['time_of_scrapping'], format='%Y%m%d %H:%M:%S')

    
    return df
    
    
def database_loading(df):
    
    
    
    # ======================== Database inserction =======================================



    # Fazendo a conexão sqlalchemy com o banco de dados recém criado
    
    connection = create_engine('sqlite:///books_database.sqlite')

    
    # Inserindo os dados de coleta via webscrapping no bando de dados
    
    df.to_sql('livros_vitrine', con=connection, if_exists='append', index=False)

    return None


if __name__ == '__main__':
    # constants:
    url = 'https://books.toscrape.com/'

    # logging:    
    path = '/home/gustavo/repos/Book-World/'    
        
    # verificar a existencia da pasta 'Logs'
    if not os.path.exists(path+'Logs'):
        os.makedirs(path+'Logs')
        
    # Definição dos Loggings:
    logging.basicConfig(
        filename= path + 'Logs/etl_BookWorld_log.txt',
        level=logging.DEBUG,
        format= '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    logger = logging.getLogger('BookWorld')
    
    
    # data collection:
    df_raw = data_collection(url)
    logger.info('data collection status: COMPLETED')
    
    
    # data transformation:
    df = data_transformation(df_raw)
    logger.info('data transformation status: COMPLETED')
    
   
    # database loading:
    database_loading(df)
    logger.info('database loading status: COMPLETED')


     