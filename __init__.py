# -*- coding: utf-8 -*-
__author__ = 'lucassilva'

import numpy
import os
#import math
import time
import sys
import re
from glob import glob
import thread,time
import multiprocessing
from multiprocessing import Manager


class thread_processador:
    ''' Passe essa classe como herança para um script qualquer que deseja ser paralelizado. Nao se esquecendo de
    sobrescrever o metodo funcao. Os resultados da funcao devem ser adicionados a self.saida_unica.append()
    '''
    from itertools import count

    '''A funcao(self,inicio,fim) deve ser sobrescrita utilizando '''
    def __init__(self,file,n_process,saida_nome):
        ''' Divide a tarefa em vários processadores diferentes
        Parametros
        ----------
        file : string
            Arquivo de entrada que será fatiado e dividido entre os processadores
        n_process : int
            Numero de processadores utilizados na paralelização
        saida_nome
            Nome do arquivo de saida
            '''
        __author__ = 'lucas'
        import thread,time

        '''script generico para fazer uma tread em um arquivo com varias entradas'''
        self.jobs=[]
        self.numero_threads= n_process
        self.arquivo_escolhido= file
        self.saida_nome=saida_nome
        self.manager= Manager()
        self.saida_unica=self.manager.list()
        self.evento_unico=self.manager.Namespace()
        self.evento_unico.x=True
        #abrindo o arquivo desejado
        arquivo = open(self.arquivo_escolhido,'r').read() #sempre mude para seu arquivo
        #salvando o arquivo desejado
        self.saida = open(self.saida_nome,'w') #sempre mude para seu arquivo de saida
        #guardando o arquivo em um array para que seja feita a divisao
        self.arquivo_array = [ arquivo_linha for arquivo_linha in arquivo.split('\n') if arquivo_linha ]
        tamanho_threads= len(self.arquivo_array)
        range_threads= tamanho_threads/self.numero_threads
        #print range_threads
        #
        for thread_numero in range (0,self.numero_threads):
           # print thread_numero*range_threads,thread_numero*range_threads+range_threads-1 #estou perdendo o ultimo valor da tabela
            if thread_numero == 0:
                inicio_valor=0
            else:
                inicio_valor=(thread_numero*range_threads)+1


            if thread_numero == 0 :
                fim_valor = range_threads
            elif thread_numero != 0 and thread_numero != self.numero_threads-1:
                fim_valor= ((thread_numero*(range_threads)) + range_threads)+1
            else:
                fim_valor = len(self.arquivo_array)-1

            #print thread_numero, inicio_valor, fim_valor


            p = multiprocessing.Process(target=self.funcao, args=(inicio_valor,fim_valor))
            self.jobs.append(p)
            p.start()
        for processos in self.jobs:
            processos.join()
        #print 'final',len(self.saida_unica)
        for i in range(len(self.saida_unica)):
            for linha_i in self.saida_unica[i]:
                self.saida.write(linha_i+'\n')

    def set_saida(self,array_para_entrar):
        self.saida_unica.append(array_para_entrar)


    def funcao(self,inicio,fim):#teste
        '''funcao generica que recebe uma parcela do arquivo completo
        Parametros
        __________
        inicio : int
            inicio da fatia do arquivo original
        fim : int
            Fim da fatia do arquivo original
        '''
        self.array_temporario=[]
        '''sobrescreva esse metodos'''
        self.arquivo_para_cruzar=open('arquivo_para_abrir','r').read()
        print 'esse metodo deve ser sobrescrito com os parametros presentes nesse arquivo dentro da classe'
        for array_linha in self.arquivo_array[int(inicio):int(fim)]:

            array_temporario.append(array_linha) # funcao que joga o resultado do seu processamento no array compartilhado entre os processadores

        self.saida_unica.append(self.array_temporario)

        # for escrever in self.array_temporario: #isso deve ser executado apenas no final da funcao
        #     self.saida.write(escrever+'\n')


    def set_false_eventos_unicos(self):
        self.evento_unico.x=False

def paralel_task(n_process):
    '''@Decorator
    Todas as funções decoradas devem obrigatoriamente conter o campo *entrada* como um de seus
    parametros. Esse decorador parte do principio que um arquivo paralelizavel entrara na função e uma
    saida será gerada.
    n_process : int
         Numero de processadores utilizados para a paralelização '''
    def decorator_funcao(funcao):
        def wrapfunc(*args, **kargs):
            #def __init__(file,n_process,saida_nome):
            #print kargs
            jobs=[]
            numero_threads= n_process
            #print kargs
            #chave de entrada do argumento da funcao generica
            arquivo_escolhido= kargs['entrada']
            manager= Manager()
            # saida_unica=manager.list()
            evento_unico=manager.Namespace()
            evento_unico.x=True
            #abrindo o arquivo desejado
            try:
                arquivo = iter(arquivo_escolhido)
            except TypeError, te:
                print some_object, 'Entre com uma variavel de entrada iterble'
                print sys.exit()
            #guardando o arquivo em um array para que seja feita a divisao
            arquivo_array = [ arquivo_linha for arquivo_linha in arquivo if arquivo_linha ]
            tamanho_threads= len(arquivo_array)
            range_threads= tamanho_threads/numero_threads

            #
            for thread_numero in range (0,numero_threads):
               # print thread_numero*range_threads,thread_numero*range_threads+range_threads-1 #estou perdendo o ultimo valor da tabela
                if thread_numero == 0:
                    inicio_valor=0
                else:
                    inicio_valor=(thread_numero*range_threads)
                if thread_numero == 0 :
                    fim_valor = range_threads
                elif thread_numero != 0 and thread_numero != numero_threads-1:
                    fim_valor= ((thread_numero*(range_threads)) + range_threads)
                else:
                    fim_valor = len(arquivo_array)+1

                p = multiprocessing.Process(target=funcao, kwargs= {'entrada':arquivo_array[inicio_valor:fim_valor]})
                p.start()
                jobs.append(p)

            deltatime= time.time()


            for processos in jobs:
                processos.join()

        return wrapfunc
    return decorator_funcao

def paralel_decorator(n_process):
    '''@Decorator
        Todas as funções decoradas devem obrigatoriamente conter o campo *entrada* e *saida* como um de seus
        parametros. Esse decorador parte do principio que um arquivo paralelizavel entrara na função e uma
        saida será gerada.
            n_process : int
                Numero de processadores utilizados para a paralelização '''

    def decorator_funcao(funcao):
        def wrapfunc(*args, **kargs):
            #def __init__(file,n_process,saida_nome):
            print kargs
            jobs=[]
            numero_threads= n_process
            #print kargs
            #chave de entrada do argumento da funcao generica
            arquivo_escolhido= kargs['entrada']
            saida_nome=kargs['saida']
            manager= Manager()
            # saida_unica=manager.list()
            evento_unico=manager.Namespace()
            evento_unico.x=True
            #abrindo o arquivo desejado
            arquivo = open(arquivo_escolhido,'r').read() #sempre mude para seu arquivo
            #salvando o arquivo desejado
            saida = open(saida_nome,'w') #sempre mude para seu arquivo de saida
            #guardando o arquivo em um array para que seja feita a divisao
            arquivo_array = [ arquivo_linha for arquivo_linha in arquivo.split('\n') if arquivo_linha ]
            tamanho_threads= len(arquivo_array)
            range_threads= tamanho_threads/numero_threads
            nome_arquivos_para_merge=[]
            nome_arquivos_temp=[]
            #print range_threads
            #
            for thread_numero in range (0,numero_threads):
               # print thread_numero*range_threads,thread_numero*range_threads+range_threads-1 #estou perdendo o ultimo valor da tabela
                if thread_numero == 0:
                    inicio_valor=0
                else:
                    inicio_valor=(thread_numero*range_threads)
                if thread_numero == 0 :
                    fim_valor = range_threads
                elif thread_numero != 0 and thread_numero != numero_threads-1:
                    fim_valor= ((thread_numero*(range_threads)) + range_threads)
                else:
                    fim_valor = len(arquivo_array)+1
                #criando_arquivo_fatiado

                arquivo_nome_temp='{}_{}_{}_{}.temp_paralelo'.format(thread_numero, inicio_valor, fim_valor, funcao.__name__)
                temp_saida=open(arquivo_nome_temp,'w')
                temp_escrever= '{}.tempwrite_paralelo'.format(arquivo_nome_temp)
                nome_arquivos_para_merge.append(temp_escrever) #salvando arquivos para fazer o merge
                nome_arquivos_temp.append(arquivo_nome_temp) # salvando nome dos arquivos temporarios

                for l_index, linha_write in enumerate(arquivo_array[inicio_valor:fim_valor]):

                    if l_index == (fim_valor-1):
                        temp_saida.write('{}'.format(linha_write))
                    else:
                        temp_saida.write('{}\n'.format(linha_write))


                p = multiprocessing.Process(target=funcao, kwargs= {'entrada':arquivo_nome_temp, 'saida':temp_escrever})
                p.start()
                jobs.append(p)
            deltatime= time.time()


            for processos in jobs:
                processos.join()

            #merging
            print time.time()-deltatime
            os.system('cat {ins} > {out}'.format(ins=' '.join(nome_arquivos_para_merge), out=saida_nome))
            #removefiles write
            #removefiles temp
            os.system('rm {files}'.format(files=' '.join(nome_arquivos_para_merge)))
            os.system('rm {files}'.format(files=' '.join(nome_arquivos_temp)))

        return wrapfunc
    return decorator_funcao

def directory_paralel_decorator(n_process, extention):
    '''@Decorator
        Todas as funções decoradas devem obrigatoriamente conter o campo *entrada* .Esse decorador
        distribui uma quantidade parecida de cada arquivo dentro *diretorio* para um processador
            n_process : int
                Numero de processadores utilizados para a paralelização
            extention : str
                Final da extensão a ser processada dentro do diretorio
                '''

    def decorator_funcao(funcao):
        def wrapfunc(*args, **kargs):
            #def __init__(file,n_process,saida_nome):

            jobs=[]
            numero_threads= n_process
            #processando entrada de kargs['diretorio'] para retirar uma ultima '/' caso lea exista
            arquivo_escolhido= re.sub('/$','',kargs['diretorio'])
            #saida_nome=kargs['saida']
            manager= Manager()
            # saida_unica=manager.list()
            evento_unico=manager.Namespace()
            evento_unico.x=True
            #abrindo o arquivo desejado
            arquivo_array = glob('{dir}/*.{ext}'.format(dir=arquivo_escolhido, ext=extention.replace('^.','')))
            #print 'tamanho: {}'.format(len(arquivo_array))
            #salvando o arquivo desejado
            #guardando o arquivo em um array para que seja feita a divisao
            tamanho_threads= len(arquivo_array)
            range_threads= tamanho_threads/numero_threads
            nome_arquivos_para_merge=[]
            nome_arquivos_temp=[]
            #print range_threads
            #
            for thread_numero in range (0,numero_threads):
               # print thread_numero*range_threads,thread_numero*range_threads+range_threads-1 #estou perdendo o ultimo valor da tabela
                if thread_numero == 0:
                    inicio_valor=0
                else:
                    inicio_valor=(thread_numero*range_threads)
                if thread_numero == 0 :
                    fim_valor = range_threads
                elif thread_numero != 0 and thread_numero != numero_threads-1:
                    fim_valor= ((thread_numero*(range_threads)) + range_threads)
                else:
                    fim_valor = len(arquivo_array)+1
                #print inicio_valor, fim_valor , len(arquivo_array[inicio_valor:fim_valor])

                p = multiprocessing.Process(target=funcao, kwargs= {'diretorio':'\t'.join(arquivo_array[inicio_valor:fim_valor])})
                p.start()
                jobs.append(p)
            for processos in jobs:
                processos.join()


        return wrapfunc
    return decorator_funcao

@paralel_decorator(10)
def teste (entrada, saida):
    escrever_disco= open(saida,'w')
    for x in open(entrada,'r').read().split('\n'):
        if len(x.split('\t'))> 4 and re.search('pseudogene',x):
            escrever_disco.write('{}\n'.format(x))

@directory_paralel_decorator(4, 'apagar')
def diretorio_teste(diretorio):

    for dir_file in diretorio.split('\t'):
        print dir_file
        os.system('head -n 1 {file}'.format(file=dir_file))

def fix_bed12_strand_error(gtf_table_format):
    '''Given a bed12 file, search for non compatible lines (end > start) and fix it
    ex:
    chr1	169857816	169822913	ENST00000367770.1	100	-	169857816	169822913	255,0,0	13	213,186,114,57,103,112,78,140,185,172,162,695,699	0,-10042,-12698,-14980,-18421,-19748,-21780,-24307,-26063,-29635,-32880,-34406,-35602
    chr1	169764180	169798955	ENST00000413811.2	100	+	169764180	169798955	255,0,0	11	174,40,105,141,166,85,139,81,65,149,552	0,3879,7581,8129,9035,10964,12751,26639,28368,32011,34223

    '''
    return_table=[]
    for line in gtf_table_format:
        if line:
            strand=  line.split('\t')[5]
            start =  int(line.split('\t')[1])
            stop =  int(line.split('\t')[2])
            if stop > start :
                #inverting coord
                stop, start = start, stop
                tabbed_line =  line.split('\t')
                #fixing position
                tabbed_line[1]=start
                tabbed_line[2]=stop
                tabbed_line[10] = tabbed_line[10].replace('-','')
                return_table.append("\t".join(tabbed_line))
            else:
                return_table.append(line)
    return return_table

def abrir_arquivo(nome_do_arquivo, delimitador='\n', tabulador_interno='none'):
    '''Abre um arquivo e retorna um array contendo cada linha lida como um index desse array.
    Args:
    nome_nome_do_arquivo (string)= caminho do arquivo a ser aberto.
    delimitador (string) = delimitador que define o final de cada linha dentro do arquivo. defaut ('\n')
    tabulador_interno (string) =  Usado para utilizar um delimitador a nivel de linha. Criando um array com a linha splitada
    pelo delimitador escolhido. defaut('none')
                                                t (tab)
                                                s(espaço)
                                                ;(ponto e virgula)
                                                :(dois pontos)
                                                n (enter)
                                                '''
    if tabulador_interno == 'none':
        return [linha for linha in open(nome_do_arquivo, 'r').read().split(delimitador) if linha]
    elif tabulador_interno == 't':
        return [linha.split('\t') for linha in open(nome_do_arquivo, 'r').read().split(delimitador) if linha]
    elif tabulador_interno == 's':
        return [linha.split(' ') for linha in open(nome_do_arquivo, 'r').read().split(delimitador) if linha]
    elif tabulador_interno == ';':
        return [linha.split(';') for linha in open(nome_do_arquivo, 'r').read().split(delimitador)if linha]
    elif tabulador_interno == ':':
        return [linha.split(':') for linha in open(nome_do_arquivo, 'r').read().split(delimitador) if linha]
    elif tabulador_interno == ':':
        return [linha.split('\n') for linha in open(nome_do_arquivo, 'r').read().split(delimitador) if linha]
    else:
        sys.stderr.write('Erro:\nEntre com um delimitador de linhas aceitavel: \n      t (tab) \n      s(espaço) \n'
                         '      ;(ponto e virgula) \n      :(dois pontos) \n      n(enter)\n')
        sys.exit(1)

def get_ENSEMBL_using_genename(gene_names, reference_gtf):
    '''Dado um array com valores de geneName e uma refenrecia GTF, retorna um array  sendo cada linha o ENSEMBL id e o
        gene_name correspondente.

    Parametros:
            gene_names (array) =  Um array contendo gene_name unicos
            reference_gtf (string) = GTF contendo o ID e ENSEMBL_ID

    Return:
            array contendo um arrays com ENSEMBL e gene_id
    '''
    gtf_file = abrir_arquivo(reference_gtf, tabulador_interno='t')
    hash_gene_ensembl={}
    for gene_name in set(gene_names):
        for linha in gtf_file:
            if 'gene_name' in linha[8] and gene_name == re.search('gene_name "(\S*)"', linha[8]).group(1):


                if gene_name in hash_gene_ensembl:
                    hash_gene_ensembl[gene_name].append(linha[8].split('transcript_id ')[1].split(';')[0].replace('"',''))
                else:
                    hash_gene_ensembl[gene_name]=[]
                    hash_gene_ensembl[gene_name].append(linha[8].split('transcript_id ')[1].split(';')[0].replace('"',''))

    return [[ensembl, x]for x in hash_gene_ensembl for ensembl in  set(hash_gene_ensembl[x])]

def funde_valores_duas_tabelas(tabela_query, tabela_target):
    ''' Procura os valores da primeira coluna da *tabela_query* em qualquer posição na linha da *tabela_target* não splitada
        Parametros
        ----------------
        tabela_query : array
            Para cada valor dentro da primeira coluna dessa tabela, procura esse valor no corpo da *tabela_query*
        tabela_target : array
            Tabela com valores que possivelmente são encontrados na tabela_query sem split interno com a linha em uma
            string completa
        Return
        ----------------
            array com a fusão das duas tabelas

    '''
    if len(tabela_query[0]) > 1:
        return [ alvo+'\t'+'\t'.join(query) for query in tabela_query for alvo in tabela_target if query[0] in  alvo]
    else:
        return [ alvo+'\t'+query for query in tabela_query for alvo in tabela_target if query in  alvo]

def captura_linhas_com_um_valor_igual_em_diferentes_arquivos(arquivo_base, coluna_id_base, arquivo_target, verse= True
                                                             , return_target=True, cap_linha_no_target=-1):
    ''' Dado o *arquivo_base* e o *arquivo_target* procura o valor contido na *coluna_id_base* do *arquivo_base* em qualquer
    campo das linhas do *arquivo_target*
    Parametros
    -------------
        arquivo_base : string
            Arquivo contendo uma coluna com ids que devem ser procurados no *arquivo_target*
        arquivo_target : string
            Arquivo onde o padrão contido na *coluna_id_base* do *arquivo_base* será buscado
        coluna_id_base : int(0-based)
            Coluna no *arquivo_base* em que o id será procurado no *arquivo_taget*. O id deve existir em ambos os
            arquivos
        verse : bool
            Se *verse* for igual a False, retorna apenas as linhas não encontradas (default= True)
        return_target : Bool
            True=Retorna na saida o arquivo target
            False= Retorna na saida o arquivo base
        cap_linha_no_target : int defaut (-1)
            adiciona uma informação contida em uma colubna especifica do arquivo target ao final da impressão das
            saidas query encontradas na busca. O valor int corresponde ao numero da coluna que será capturada,
            sendo essa previamente separada por \tab
    Return
    -------------
    Retorna um array com  as linhas do *arquivo_target* que possuem o id do *arquivo_base*
    '''

    convertendo = coluna_id_base
    coluna_id_base = int(convertendo )
    base= abrir_arquivo(arquivo_base, tabulador_interno='t')
    target = abrir_arquivo(arquivo_target)
    if verse:
        if return_target:
            return [linha for id in base for linha in target if id[coluna_id_base] in linha]
        else: #retorna o arquivo base
            if cap_linha_no_target == -1: # não retorna com a adição da ultima linha do target
                return [id for id in base for linha in target if id[coluna_id_base] in linha]
            else: #retorna com a adicao da ultima linha sendo um valor determinado dentro do target
                return [('\t'.join(id)+ '\t' + linha.split('\t')[cap_linha_no_target]).split('\t')
                        for id in base for linha in target if id[coluna_id_base] in linha]
    else: # retorna as linhas que não foram encontradas
            return [linha for id in base for linha in target if id[coluna_id_base] not in linha]

def retorna_somatoria_de_uma_coluna_especifica(arquivo, id_unico_para_fusao , id_da_coluna_somatoria, media=False):
    ''' Dado uma tabela com valores não únicos, mas com ID unicos em uma determinada coluna, Retorna a somatória
    do valor inteiro de uma das colunas existentes

    Parâmetros
    -----------
        nome_do_arquivo : string
            Arquivo possuindo ids repetidos, mas com algum identificador único em uma das colunas. Essa coluna deve ser
            separada por tab.
        id_unico_para_fusao : int
            Numero da coluna [zero-based] em que será procurado os id para fundiar as linhas não unicas
        id_da_coluna_somatoria : int
            Numero da coluna [zero-based] em que os valores serão fundidos entre os elementos com o mesmo *id_unico_para_fusao*
        media : bool
            If true, return the mean of sum

        Retorno:
        ----------
            array [(id_unico, somatoria)]
        '''
    tabela=abrir_arquivo(arquivo, tabulador_interno='t')
    hash_tabela = {}
    for linha in tabela:
        if media == False:
            if linha[id_unico_para_fusao] in  hash_tabela:
                hash_tabela[linha[id_unico_para_fusao]] += float(linha[id_da_coluna_somatoria])
            else:
                hash_tabela[linha[id_unico_para_fusao]] = float(linha[id_da_coluna_somatoria])
        if media == True:
            if linha[id_unico_para_fusao] in  hash_tabela:
                hash_tabela[linha[id_unico_para_fusao]].append(float(linha[id_da_coluna_somatoria]))
            else:
                hash_tabela[linha[id_unico_para_fusao]]=[]
                hash_tabela[linha[id_unico_para_fusao]].append(float(linha[id_da_coluna_somatoria]))
    if media==False:
        return sorted([[x, hash_tabela[x]] for x in hash_tabela], key=lambda valor: valor[1],reverse=True)
    else:
        return sorted([[x, numpy.mean(hash_tabela[x])] for x in hash_tabela], key=lambda valor: valor[1],reverse=True)

def all_dir_wigfix_to_bigwig(diretorio, chrom_sizes_file):
    """ Converte todos os arquivos(wigfix) em um determinado diretorio para o formato bigwig
    Parametros
    ----------
    diretorio : string
        Diretorio contendo os arquivos wigfix ex: /home/arquivos/
    chrom_sizes_file : string
        Nome do arquivo com o tamanho dos cromossomos (utilizar ferramenta fetchChromSizes)

    """

    arquivos= glob('{}*.wigFix'.format(diretorio))
    print arquivos

    for arquivo in arquivos:
        print 'converting {}...'.format(arquivo)
        os.system('wigToBigWig {arquivo_x} {chrom_sizes_file} {saida}'.format(arquivo_x=arquivo,
                                                            chrom_sizes_file=chrom_sizes_file,
                                                            saida="{}.bw".format(arquivo.split('/').pop().rsplit('.',1)[0]))
                                                            )

def string_window_generator(string_in, window):
    '''Retorna um generator dado uma string e um intervalo desejado

    Parametros
     ----------
    string_in: str
        String base onde a janela será cria
    window : int
        Intervalo entre cada fatia da string

    return
    -----------
        Tuple com o inicio e final da janela
        ex: [0,10]
     '''
    for index, valor in enumerate(string_in):
        if (index+window)<= len(string_in)-1:
             yield [index, index+window]

def ngsutils_all_dir_count(diretorio, gtf):
    '''faz a contagem e cobertura de reads dado diretorio com arquivos bam e um arquivo gtf
    Parametros
    ----------
    diretorio : string
        Diretorio onde os arquivos .bam estão presentes (obs: todos os arquivos bam dentro dessa pasta serão
        utilizados)
    gtf : string
        Caminho completo para o arquivo gtf
    '''

    diretorio_bai =  glob('{}*.bai'.format(diretorio))
    diretorio_bam =  glob('{}*.bam'.format(diretorio))

    for contador,bam in enumerate(diretorio_bam):
        #testando existencia dos arquivos index .bai
        sys.stdout.write('{}\{} concluido...\n'.format(contador, len(diretorio_bam)))
        if bam.replace('.bam','.bai') in diretorio_bai:
            #caso exista o arquivo index (bai)
            sys.stderr.write('arquivo index (bai) encontrado {}...\n iniciando analise...\n'.format(bam))
            os.system('bamutils count'
                      ' -gtf {gtf_caminho}'
                      ' -coverage'
                      ' -fpkm'
                      ' -norm mapped '
                      '{bam_caminho} > {fpkm_saida}'.format(gtf_caminho=gtf, bam_caminho=bam, fpkm_saida=bam.replace('.bam','.fpkm_table')))
        #criando arquivo bai
        else:
            sys.stderr.write('creating bai to file {}...\n'.format(bam))
            os.system('samtools index {}'.format(bam))
            os.system('bamutils count'
                      ' -gtf {gtf_caminho}'
                      ' -coverage'
                      ' -fpkm'
                      ' -norm mapped '
                      '{bam_caminho} > {fpkm_saida}'.format(gtf_caminho=gtf, bam_caminho=bam, fpkm_saida=bam.replace('.bam','.fpkm_table')))

def bed_start_site_to_tss(bed_string, down = 1000, up = 1000, not_reverse= True):
    '''dado uma coordenada *bed_string* retorna aquela string com o inicio e fim da coordenada sendo a extensão
        do start site dessa sequencia dada
        Parametros:
        -------------------
        bed_string : string bed separada por tab
            String no formato bed
        down/up : int defaut(1000)
            Quantidade de nuceotideos que serão extendidas partindo do start site do bed dado
        not_reverse : true
            Deixa os valores com coordenadas menores na primeria posição mesmo que a fita seja negativa
        '''
    saida_str = ''
    if '\t-\t' in bed_string:
        bed_string_tabulada =  bed_string.split('\t')
        ref_coord = int(bed_string_tabulada[2])
        n_start= ref_coord + up
        n_end= ref_coord - down
        bed_string_tabulada[1]= str(n_start)
        bed_string_tabulada[2]= str(n_end)
        #print (n_start)-(n_end)
        saida_str = '\t'.join(bed_string_tabulada)
    else:
        bed_string_tabulada =  bed_string.split('\t')
        ref_coord = int(bed_string_tabulada[1])
        n_start= ref_coord - down
        n_end= ref_coord + up
        bed_string_tabulada[1]= str(n_start)
        bed_string_tabulada[2]= str(n_end)
        saida_str= '\t'.join(bed_string_tabulada)

    saida_splitada= saida_str.split('\t')

    if int(saida_splitada[1]) >= 0 and  int(saida_splitada[2]) >= 0 : #caso nao exista valores negativos
        if not_reverse:
            if int(saida_splitada[1]) > int(saida_splitada[2]):
                temp1= str(saida_splitada[1])
                temp2= str(saida_splitada[2])
                saida_splitada[1]=temp2
                saida_splitada[2]=temp1
                return '\t'.join(saida_splitada)
            else:
                return saida_str
        else: #caso não queira o dado modificado para reversed
            return saida_str
    else:
        sys.stderr.write('valor proximo da borda do cromossomo: '+ saida_splitada[1]+'\n')

def main():
    '''Projeto no github..tentando atualizar'''


if __name__ == '__main__':
    sys.exit(main())
