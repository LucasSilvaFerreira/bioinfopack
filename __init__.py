# -*- coding: utf-8 -*-
__author__ = 'lucassilva'

import numpy
import os
#import math
import time
import sys
import re
from glob import glob
import thread, time
import multiprocessing
from multiprocessing import Manager
import pybedtools
import subprocess
import os.path

class AppNotFound(Exception):
    def __init__(self, file_name):
        sys.stderr.write('\nApp: {} NOT FOUND\n'
                         'Try to install, fix or use a path APP parameter!\n'.format(file_name))
        sys.exit()

class DirNotFound(Exception):
    def __init__(self, dir_name):
        sys.stderr.write('\nDIR: {} NOT FOUND\n'
                         'Check your dir name!\n'.format(dir_name))
        sys.exit()

class FileNotFound(Exception):
     def __init__(self, file_name):
        sys.stderr.write('\nFile: {} NOT FOUND\n'
                         'Check your path name!\n'.format(file_name))
        sys.exit()





class ExperimentDesignError(Exception): pass


class Blast_query_parser:
    def __init__(self, query_id,
                       database_origin,
                       query_size=0,
                       sig_alings_list=None,
                       hits=False):

        self.query_id = query_id
        #print self.query_id
        self.query_size = query_size
        self.database_origin = database_origin
        self.sig_aling_list = sig_alings_list #file that will be transformed in  hash

        if self.sig_aling_list is not None:
         self.alings_hits_array_hashs = self.parse_alings(self.sig_aling_list)
        #criar variavel hits
         self.size = len(self.alings_hits_array_hashs)
        else:
            self.size=0
        self.index=0
    def parse_alings(self, sig_aling_list):
        array_temp_return_parser_aling=[]

        for aling_data in sig_aling_list:
            busca = re.search('(\S*)\s*(\S*)\s*(\S*)', aling_data)
            array_temp_return_parser_aling.append({'id':busca.group(1), 'score': busca.group(2), 'e_value':busca.group(3)})

        return array_temp_return_parser_aling

        #return array_temp_return_parse_aling
        #recebe hits positivos. cehcar se o tamanho é maior que zero(hits =True)fazer uma HASH com o score, e-value, identidade, gaps, matchs
        #fazer troço do print modificado

    def __iter__(self):
        return self

    def next(self):
        if self.index < self.size:
            i= self.index
            self.index += 1


            return self.alings_hits_array_hashs[i]
        else:
            raise StopIteration()


class Blast_parser:
    def __init__(self, blast_in):
        ''' receive a parse out file'''
        self.i=0
        self.blast_in = blast_in
        #parsing file by 'Query= ' tag
        self.Parser_open = abrir_arquivo(self.blast_in, delimitador='Query= ')[1:-1]
        self.created_objects = self.create_objects(self.Parser_open)
        self.size = len(self.created_objects)

    def create_objects(self, array_parse_open):
        array_query_return = []
        for query in array_parse_open:
            id = query.split('\n')[0]
            print id
            size = re.search(' +\((\d+) ', query.split('\n')[1].replace(',','')).group(1)
            database = query.split('Database: ')[1].split('\n')[0]
            try:
                sig_alings = query.split('Sequences producing')[1].split('>')[0].split('\n')[2:-2]

                created_object=Blast_query_parser(query_id= id, query_size= size, database_origin=database, hits=True, sig_alings_list=sig_alings)
            except:
                sys.stderr.write("sem hits" + "\n")
                created_object=Blast_query_parser(query_id= id, query_size= size, database_origin=database, hits=False, sig_alings_list=None)

            array_query_return.append(created_object)

        return array_query_return

    def __iter__(self):
        return self

    def next(self):
        if self.i < self.size:
            i= self.i
            self.i += 1
            return self.created_objects[i]
        else:
            raise StopIteration()


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

            self.array_temporario.append(array_linha) # funcao que joga o resultado do seu processamento no array compartilhado entre os processadores

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
                print te, 'Entre com uma variavel de entrada iterble'
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

def shellToString(string_comando):
    '''Retorna em forma de lista uma saida do comando do sistema invocado, utilizando a *string_comando* escolhido.'''
    return  [saida_comando for saida_comando in subprocess.check_output(string_comando, shell = True, stderr = subprocess.STDOUT).split("\n")]

def intersect_interaction_data(interaction_file, bed_file, bed_col=None):
    ''' Given a tabular *interaction_file*, returns a chosen line or  coord in bed file  that interacts with one o both interaction file coords.
    Parameters
    ----------------------
    interaction_file(file or array tab-sep):
        File that follow the example (2 coordinates followed):

        chr1    1   100 chr1    500 600 geneA   90  98  id .. .. ..
    bed_file (bed_file):
        Some bed file
    bed_col (int):
        When the intersect between bed and intersect file exists. Returns the bed line number given by *bed_col*.

    Returns (array):

        Vector containing the example out:
         chr1   1   100 x,y,z chr1 5 500 a,b,c
         chr2 1 200 a chr2 500 1000 z,x,r
    '''
    #Checking interaction_file type
    sys.stderr.wirte("FUnção ainda não terminada " +"\n")
    if type(interaction_file) == str:
         inter_file = abrir_arquivo(interaction_file, tabulador_interno='t')
    elif type(interaction_file) == list:
         inter_file = interaction_file
    else:
        sys.stderr('No recognize file Type' + '\n')
        sys.exit(1)
    inter_a = [i_a[0:3]+i_a[3:6]+i_a[6:-1] for i_a in inter_file]
    inter_b = [i_b[3:6]+i_b[0:3]+i_a[6:-1]for i_b in inter_file]
    #converting bed file to pybed object
    bed_file_pybt_object = pybedtools.BedTool(bed_file)
    #remove pybedtools temps
    pybedtools.cleanup()
    #Parsing table to bed
    inter_a_bed = pybedtools.BedTool('\n'.join('\t'.join(parse_bed_a) for parse_bed_a in inter_a if not re.search('\D',parse_bed_a[1])), from_string=True)
    inter_b_bed = pybedtools.BedTool('\n'.join('\t'.join(parse_bed_b) for parse_bed_b in inter_b if not re.search('\D',parse_bed_b[1])), from_string=True)
    #Intersecting A and B interaction with bed file
    result_bed_a_intersect = inter_a_bed.intersect(bed_file_pybt_object)
    result_bed_b_intersect = inter_b_bed.intersect(bed_file_pybt_object)
    print bed_file_pybt_object.intersect(inter_a_bed, wa=True, wb=True)
    print '-------------------------------------'
    print bed_file_pybt_object.intersect(inter_b_bed, wa=True, wb= True)

def generate_bed_bins(bin_size, chr_name, start_chr, end_chr, force_end=0):
    '''Given a bin size  creates a bed_string file
    Parameters
    -----------------
    bin_size: int
        Fragment the given chr in N *bin_size*
    chr_name : str
        chromossome name
    start_chr : int
        The initial bin coord
    end_chr : int
        The end bin coord (The last bin always will be lost).
    force_end : int (defaut 0)
        get the last bin and set a custom value.
    Return
    ---------------
    One string whith bed file.
    '''
    bin_size, start_chr, end_chr = int(bin_size), int(start_chr), int(end_chr)
    bed_list=  ['\t'.join([chr_name, str(range_coord), str((range_coord+bin_size)-1)]) for range_coord in range(start_chr, end_chr, bin_size)]

    if force_end != 0:
        #set a custom end coord in last bed_list value
        editing_last = bed_list[-1].split('\t')
        editing_last[2] = str(force_end)
        bed_list[-1] = '\t'.join(editing_last)
    return '\n'.join(bed_list)

def fix_bed6_strand_error(bed6_file_list):
    '''Given a bed6 file (3 line min size). Fix that lines which have stop  small than starts
     Parameters
     _____________
     bed6_file_array (list_tab_sep)

     Returns:
     _____________
     array that bed6 file was fixed
     '''
    out_return = []
    for line in bed6_file_list:
        if len(line) >=3:
            if int(line[1]) < int(line[2]):
                #Inverting data
                line[1],line[2] = line[2],line[1]
            out_return.append(line)

def fix_bed12_strand_error(gtf_table_format):
    '''Given a bed12 file, search for non compatible lines (end > start) and fix it
    ex:
    chr1	169857816	169822913	ENST00000367770.1	100	-	169857816	169822913	255,0,0	13	213,186,114,57,103,112,78,140,185,172,162,695,699	0,-10042,-12698,-14980,-18421,-19748,-21780,-24307,-26063,-29635,-32880,-34406,-35602
    chr1	169764180	169798955	ENST00000413811.2	100	+	169764180	169798955	255,0,0	11	174,40,105,141,166,85,139,81,65,149,552	0,3879,7581,8129,9035,10964,12751,26639,28368,32011,34223

    '''
    return_table=[]
    for n_line, line in enumerate(gtf_table_format):
        if line:
            if len(line) < 10:
                sys.stderr.write( 'Line: {} has a incompatible bed12 size.\n'.format(str(n_line)))
                pass
            strand=  line[5]
            start =  int(line[1])
            stop =  int(line[2])
            if stop < start :
                #inverting coord
                stop, start = start, stop
                tabbed_line =  line
                #fixing position
                tabbed_line[1]=str(start)
                tabbed_line[2]=str(stop)
                tabbed_line[11] = tabbed_line[11].replace('-','')
                return_table.append("\t".join(tabbed_line))
            else:
                line[11] = line[11].replace('-', '')
                return_table.append("\t".join(line))
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
    elif tabulador_interno == 'n':
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
        return [ alvo+'\t'+'\t'.join(map(str, query)) for query in tabela_query for alvo in tabela_target if query[0] in  alvo]
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
    print 'carregado'
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

def getSameValues(arquivo_base, coluna_id_base, arquivo_target, coluna_id_target, verse= True
                                                             , return_target=True, cap_linha_no_target=-1):
    ''' Dado o *arquivo_base* e o *arquivo_target* procura o valor contido na *coluna_id_base* do *arquivo_base* em qualquer
    campo das linhas do *arquivo_target*. Obs= Valores redundantes são considerados
    Parametros
    -------------
        arquivo_base : string
            Arquivo contendo uma coluna com ids que devem ser procurados no *arquivo_target*
        arquivo_target : string
            Arquivo onde o padrão contido na *coluna_id_base* do *arquivo_base* será buscado
        coluna_id_base : int(0-based)
            Coluna no *arquivo_base* em que o id será procurado no *arquivo_taget*. O id deve existir em ambos os
            arquivos
        coluna_id_target: int(0-based)
            Coluna no *arquivo_target* em que o valor de *arquivo_base* será procurado
        verse : bool
            Se *verse* for igual a False, retorna apenas as linhas não encontradas (default= True)
        return_target : Bool
            True=Retorna na saida o arquivo target
            False= Retorna na saida o arquivo base
        cap_linha_no_target : int defaut (-1)
            adiciona uma informação contida em uma colubna especifica do arquivo target ao final da impressão das
            saidas query encontradas na busca. O valor int corresponde ao numero da coluna que será capturada,
            sendo essa previamente separada por \tab.
            obs: Linhas em que os valores podem ser redundantes serão retornados separados por ,
    Return
    -------------
    Retorna um array com  as linhas do *arquivo_target* que possuem o id do *arquivo_base*
    '''

    sys.stderr.write("Sempre confira se as linhas com as chaves estão corretas! As hashs dependem disso :)" + "\n")
    coluna_id_base = int(coluna_id_base)
    coluna_id_target =  int(coluna_id_target)
    #checando se os arquivos são listas ou enderecos de arquivos
    if type(arquivo_base)== list:
        base = arquivo_base
    else:
        base= abrir_arquivo(arquivo_base, tabulador_interno='t')

    if type(arquivo_target) ==list:
        target = arquivo_target
    else:
        target = abrir_arquivo(arquivo_target, tabulador_interno='t')


    target_hash = {}
    for t_linha in target:
        if len(t_linha) >= coluna_id_target:
            if t_linha[coluna_id_target] in target_hash:
                #sys.stderr.write("repetido" + "\n")
                target_hash[t_linha[coluna_id_target]].append(t_linha)
            else:
                #sys.stderr.write("diferente" + "\n")
                target_hash[t_linha[coluna_id_target]]=[]
                target_hash[t_linha[coluna_id_target]].append(t_linha)
    #print 'carregado'
    target ='zerada'
    out_array_table=[]
    if verse:
        if not return_target:
            for id in base:
                if len(id)>= coluna_id_base and id[coluna_id_base] in target_hash:
                    #acrescenta cada valor dentro do array de saida
                    for valor in target_hash[id[coluna_id_base]]:
                        out_array_table.append('\t'.join(valor))
            #return [linha for id in base for linha in target if id[coluna_id_base] in linha[coluna_id_target]]

        else: #retorna o arquivo base
            sys.stderr.write("retorna arquivo target" + "\n")
            if cap_linha_no_target == -1: # não retorna com a adição da ultima linha do target
                sys.stderr.write("not capture line" + "\n")
                for id in base:
                    if len(id)>= coluna_id_base and id[coluna_id_base] in target_hash:
                        #acrescenta cada valor dentro do array de saida
                        out_array_table.append('\t'.join(id))


                #return [id for id in base for linha in target if id[coluna_id_base] in linha[coluna_id_target]]


            else: #retorna com a adicao da ultima linha sendo um valor determinado dentro do target
                sys.stderr.write("Retorna linha com adicao" + "\n")
                for id in base:
                    #print id[coluna_id_base]
                    if len(id)>= coluna_id_base and id[coluna_id_base] in target_hash:
                        #acrescenta cada valor dentro do array de saida
                        valores_colunas=[]
                        for valor in target_hash[id[coluna_id_base]]:
                            #print valor
                            #print '|------>', id, coluna_id_base, id[coluna_id_base], target_hash[id[coluna_id_base]][cap_linha_no_target]
                            valores_colunas.append(valor[cap_linha_no_target])
                        #print valores_colunas
                        id.append(','.join(valores_colunas))
                        out_array_table.append(id)



                #return [('\t'.join(id)+ '\t' + str(coluna_id_target[cap_linha_no_target]))
                #        for id in base for linha in target if id[coluna_id_base] in linha[coluna_id_target]]
    else: # retorna as linhas que não foram encontradas
        sys.stderr.write('Returning the diference between files'+ '\n')
        hash_base = {id[coluna_id_base]:id for id in base if len(id) >= coluna_id_base }
        base = 'zerada'
        for chave_target in target_hash:
            if chave_target not in hash_base:
                for valor in target_hash[chave_target]:
                    out_array_table.append('\t'.join(valor))

            #return [linha for id in base for linha in target if id[coluna_id_base] not in linha[coluna_id_target]]

    return out_array_table

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
        return sorted([[x, hash_tabela[x]] for x in hash_tabela], key=lambda valor: valor[1], reverse=True)
    else:
        for x in hash_tabela:
            #print numpy.mean(hash_tabela[x])
            return sorted([[x, numpy.mean(hash_tabela[x])] for x in hash_tabela], key=lambda valor: valor[1], reverse=True)

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


def check_program_exists(app_name):
    '''Given a app name, check if this app exists and returns the app location.
    parameter:
        app_name : string
    return: string
        String which location of this app
    exception: AppNotFound
        Raises exception and close the program.
    '''
    try:
        path_app_dir = shellToString("which {}".format(app_name))
        if len(path_app_dir[0]) > 0:
            return path_app_dir[0]
    except:
        raise AppNotFound(app_name)

def check_path_exists(path_name):
    if os.path.isfile(path_name):
        return True
    else:
        return False



def task_check_app_path(path_app, commum_name):
    '''Given a app_path check if is a valid link.
     Parameter
     ---------
     path_app : string
        Path for a valid app file
     commun_name : string
        A string with the name app to try use case the paths is unknow.
     Returns
     ---------
     returns a path_app name or raise a error case these paht not is founded
     '''
    if not path_app:
        path_app = check_program_exists(commum_name)
    else:
        if check_path_exists(path_app):
            pass
        else:
            raise AppNotFound(path_app)
    return path_app




def top_hat_function(diretorio, paired_end=True, threads=5, tophat_path=None, adapters=None):


    '''
    tophat -p {threads} -o {saida_dir} -r 150 --mate-std-dev 150 --library-type fr-secondstrand --b2-sensitive -G {gtf} {index_genome} {r1} {r2}
    '''



    """ Given a directory and files ""*fast.gz" Do a trimmomatic filter using that files
        Parametros
    ----------
    diretorio : string
        Dir with  fastq.gz files ex: /home/arquivos/
    paired_end : bool (default: true)
        The reads are paired-end?
    threads : int (default : 5)
        Number of threads used to do a trimm
    trimm_path: string (defaut: None )
        pass the path to trimmomatic. If none check for trimmomatic system path
    adapters : string
        Path for adapters in fasta file

    """




    # if adapters:
    #     if os.path.isfile(adapters):
    #         pass
    #     else:
    #         raise IOError("\n{} NOT FOUND".forma(adapters))
    #         sys.exit(0)
    # else:
    #     raise IOError("You need enter with adapter file....")
    #     sys.exit()
    #
    #
    # arquivos= glob('{}*.fastq.gz'.format(diretorio))
    #
    # if paired_end:
    #     pairs = [];
    #     for file in arquivos:
    #         for pair in arquivos:
    #             if file != pair:
    #                 if file.split('_R1')[0] == pair.split('_R2')[0]:
    #                     pairs.append([file, pair])
    #
    #     size_pairs = 0
    #
    #
    #
    #     if len(pairs) == (len(arquivos)/2):
    #         pass  # If all pairs are matched...
    #     else:
    #         pairs_to_locate = '\t'.join(['\t'.join(x_pairs) for x_pairs in pairs]).split('\t')
    #
    #         files_error = []
    #         for x in arquivos:
    #             if x not in pairs_to_locate:
    #                 files_error.append(x)
    #             else:
    #                 pass
    #
    #         files_not_found_str = '\n'.join(files_error)
    #
    #         raise ExperimentDesignError('\nThere some files without pairs!'
    #                                     ' \nRemove from directory this files or check inconsistencies in their names'
    #                                     ' \n{}'.format(files_not_found_str))
    #         sys.exit(0)
    #
    # else:
    #     print 'como proceder quando não existe pares?'
    #

    #
    #
    # count=1
    # for r1, r2 in pairs:
    #     print "Processing : {}/{}".format(count, len(pairs))
    #     files_to_trimm = '{r1} {r2} {r1_paired} {r1_unpaired} {r2_paired} {r2_unpaired}'.format(r1=r1,
    #                                                                            r2=r2,
    #                                                                            r1_paired=r1.split('.')[0]+"_paired.fq.gz",
    #                                                                            r1_unpaired=r1.split('.')[0]+"_unpaired.fq.gz",
    #                                                                            r2_paired=r2.split('.')[0]+"_paired.fq.gz",
    #                                                                            r2_unpaired=r2.split('.')[0]+"_unpaired.fq.gz"
    #                                                                            )
    #
    #
    #     command = 'java -jar {trimm_path}' \
    #     ' PE -phred33' \
    #     ' -threads {threads}' \
    #     ' {files_to_trimm}' \
    #     ' ILLUMINACLIP:{adapter}:2:30:10' \
    #     ' LEADING:3' \
    #     ' TRAILING:3' \
    #     ' SLIDINGWINDOW:4:15' \
    #     ' MINLEN:16'.format(trimm_path=trimm_path,
    #                           threads=threads,
    #                           files_to_trimm=files_to_trimm,
    #                           adapter=adapters)
    #
    #     os.system(command)
    #
    #
    #     count +=1




def all_dir_trimmomatic(diretorio, paired_end=True, threads=5, trimm_path=None, adapters=None):
    """ Given a directory and files ""*fast.gz" Do a trimmomatic filter using that files
        Parametros
    ----------
    diretorio : string
        Dir with  fastq.gz files ex: /home/arquivos/
    paired_end : bool (default: true)
        The reads are paired-end?
    threads : int (default : 5)
        Number of threads used to do a trimm
    trimm_path: string (defaut: None )
        pass the path to trimmomatic. If none check for trimmomatic system path
    adapters : string
        Path for adapters in fasta file

    """
    if adapters:
        if os.path.isfile(adapters):
            pass
        else:
            raise IOError("\n{} NOT FOUND".forma(adapters))
            sys.exit(0)
    else:
        raise IOError("You need enter with adapter file....")
        sys.exit()


    arquivos= glob('{}*.fastq.gz'.format(diretorio))
    #print arquivos
    #print arquivos
    if paired_end:
        pairs = [];
        for file in arquivos:
            for pair in arquivos:
                if file != pair:
                    if file.split('_R1')[0] == pair.split('_R2')[0]:
                        pairs.append([file, pair])
        #print pairs
        size_pairs = 0
        #print len(pairs), len(arquivos)


        if len(pairs) == (len(arquivos)/2):
            pass  # If all pairs are matched...
        else:
            pairs_to_locate = '\t'.join(['\t'.join(x_pairs) for x_pairs in pairs]).split('\t')
            #print pairs_to_locate
            files_error = []
            for x in arquivos:
                if x not in pairs_to_locate:
                    files_error.append(x)
                else:
                    pass

            files_not_found_str = '\n'.join(files_error)

            raise ExperimentDesignError('\nThere some files without pairs!'
                                        ' \nRemove from directory this files or check inconsistencies in their names'
                                        ' \n{}'.format(files_not_found_str))
            sys.exit(0)

    else:
        print 'como proceder quando não existe pares?'

    if not trimm_path:
        trimm_path = check_program_exists('trimmomatic-0.30.jar')
    else:
        if check_path_exists(trimm_path):
            pass
        else:
            raise AppNotFound(trimm_path)


    count=1
    for r1, r2 in pairs:
        print "Processing : {}/{}".format(count, len(pairs))
        files_to_trimm = '{r1} {r2} {r1_paired} {r1_unpaired} {r2_paired} {r2_unpaired}'.format(r1=r1,
                                                                               r2=r2,
                                                                               r1_paired=r1.split('.')[0]+"_paired.fq.gz",
                                                                               r1_unpaired=r1.split('.')[0]+"_unpaired.fq.gz",
                                                                               r2_paired=r2.split('.')[0]+"_paired.fq.gz",
                                                                               r2_unpaired=r2.split('.')[0]+"_unpaired.fq.gz"
                                                                               )


        command = 'java -jar {trimm_path}' \
        ' PE -phred33' \
        ' -threads {threads}' \
        ' {files_to_trimm}' \
        ' ILLUMINACLIP:{adapter}:2:30:10' \
        ' LEADING:3' \
        ' TRAILING:3' \
        ' SLIDINGWINDOW:4:15' \
        ' MINLEN:16'.format(trimm_path=trimm_path,
                              threads=threads,
                              files_to_trimm=files_to_trimm,
                              adapter=adapters)

        os.system(command)

        #print command
        count +=1



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
    if not re.search('/$', 'diretorio'):
        diretorio += '/'
    else:
        pass
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
    #print bed_string
    if "\t-" in bed_string:
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

def bed_get_midle_point(bed_string):
    '''Given a *bed_string* respecting the 5' -> 3' coordinates
    returns a relative midpoint.
    Parameters:
    -------------------
    bed_string : str
        bed_string separated  by tab

    -------------------
    string with mid point'''
    bed_string = bed_string.split('\t')
    midpoint = ((int(bed_string[2])-int(bed_string[1]))/2)
    return str(int(bed_string[1]) + midpoint)

def get_groups(array_files):
    '''pattern: 10_S2_L001_R1_001.fastq.gz
        group comfirmation: 10_S2_R1
        EX:
        10_S2_L001_R1_001.fastq.gz
        10_S2_L002_R1_001.fastq.gz
        10_S2_L003_R1_001.fastq.gz

    '''

    out_files = []
    hash_temp = {}
    for file_x in array_files:
        split_filename = file_x.split('_')
        prefix = '{}_{}_{}'.format(split_filename[0], split_filename[1], split_filename[3])
        if not prefix in hash_temp:
            hash_temp[prefix] = []
            hash_temp[prefix].append(file_x)
        else:
            hash_temp[prefix].append(file_x)

    for key, value in hash_temp.iteritems():
        print 'zcat {} >'.format(' '.join(value))

def fusion_rnaseq_samples(dir):
    if dir[-1]=='/':
        pass
    else:
        dir += '/'

    if os.path.isdir(dir):
        arquivos_glob = glob('{}*.fastq.gz'.format(dir))
        get_groups(arquivos_glob)
    else:
        raise DirNotFound("\n{} NOT found...".format(dir))

def main():
    '''Projeto no github..tentando atualizar'''

if __name__ == '__main__':
    sys.exit(main())
