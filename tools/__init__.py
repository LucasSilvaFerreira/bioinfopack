# -*- coding: utf-8 -*-
__author__ = 'lucassilva'
import sys
import os
from bioinfopack import abrir_arquivo, DirNotFound, check_program_exists, check_path_exists, AppNotFound, FileNotFound, ExperimentDesignError
import re
import logging
import datetime
from multiprocessing import Pool
from functools import partial


def execute_SYSTEM_command_task(log_file_name, tool_name_to_log, out_nohup_dir, my_task):
        actual_time = datetime.datetime.now().strftime("%I:%M h on %B %d, %Y")
        logging.basicConfig(filename=log_file_name, level=logging.DEBUG)
        nohup_out_name = out_nohup_dir+'nohup_'+re.sub("\.|-|\s|\/", '_', my_task)+'.out'
        nohup_out_name = re.sub('//+|__+', '_',nohup_out_name)[0:200]
        cmd = "nohup {my_task} > {nh}".format(my_task= my_task, nh=nohup_out_name)
        logging.debug('\n<<{}>> Run on: {}\nCommand:\n{}'.format(
            tool_name_to_log, actual_time, "{}\nNohup_FILE::\nfile-> {}".format(my_task, nohup_out_name)))
        os.system(cmd)
        finished_time = datetime.datetime.now().strftime("%I:%M  on %B %d, %Y")
        logging.debug('\n<<{}>> Finished : {}'.format(tool_name_to_log, finished_time))

def to_bool(value):
    """
       Converts 'something' to boolean. Raises exception for invalid formats
           Possible True  values: 1, True, "1", "TRue", "yes", "y", "t"
           Possible False values: 0, False, None, [], {}, "", "0", "faLse", "no", "n", "f", 0.0, ...
    """
    if str(value).lower() in ("yes", "y", "true",  "t", "1"): return True
    if str(value).lower() in ("no",  "n", "false", "f", "0", "0.0", "", "none", "[]", "{}"): return False
    raise Exception('Invalid value for boolean conversion: ' + str(value))


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

def task_check_files_path(file_path):
    if os.path.isfile(file_path):
        pass
    else:
        raise FileNotFound(file_path)
    return file_path

def task_check_dir_path(dir_path):
    if os.path.isdir(dir_path):
        pass
    else:
        raise DirNotFound(dir_path)
    return dir_path

class Tool:
    def __init__(self, tool_name, config, multi_task):
        self.multi_task = multi_task
        self.config = config
        self.tool_name = tool_name
        self.check_consistence_in_config(config)
        self.threads = config["THREADS"]
        if re.search('/$', config["PATH_DIR_OUT"]):
            pass
        else:
            self.path_out = config["PATH_DIR_OUT"] + '/'
        self.task = []
        self.out_names =[]


    def check_consistence_in_config(self, config):
        for key_file, file_path in config.iteritems():
            print key_file, file_path
            if type(file_path)!=list:
                if re.match("SKIP", file_path):
                     config[key_file]=''
            if re.match("PATH", key_file):
                if re.match("PATH_CREATE", key_file):
                    pass # checar permissoes
                elif re.match("PATH_TOOL", key_file):
                    self.tool_path = task_check_app_path(file_path, self.tool_name)

                elif re.match("PATH_INDEX", key_file):
                    pass

                elif re.match("PATH_DIR_OUT", key_file):
                    if os.path.isdir(file_path):
                        pass
                    else:
                        os.makedirs(file_path)
                else:
                    if type(file_path) == list:
                        for sub_path in file_path:
                            if re.match("PATH_DIR", key_file):

                                task_check_files_path(sub_path)
                    else:
                        if re.match("PATH_DIR", key_file):
                            task_check_dir_path(file_path)
                        else:
                            task_check_files_path(file_path)
        print 'end'


    @staticmethod
    def parser_config_file(file):
        '''Given a config file. Returns a hash whith a key and value for each input field'''
        if task_check_files_path(file):
            config_file = abrir_arquivo(file)
            hash_cfg={}
            #All character before '=' will be considered keys
            #All lines with ':' will be considered array. And the values will be pass like an array
            for cfg_line in config_file:
                cfg_line = cfg_line.strip('"')  # removing quotes
                if cfg_line > 0 and '#' not in cfg_line and '=' in cfg_line:
                    key, value = cfg_line.split('=')
                    key = key.upper()
                    value = value.replace('"', '').replace("'", '')
                    if ':' in value:
                        result = value.split(':')
                    else:
                        result = value
                    if key in hash_cfg:
                        hash_cfg[key].append(result)
                    else:
                        hash_cfg[key]=[]
                        hash_cfg[key].append(result)
            for key_parse, value_parse in hash_cfg.iteritems(): # remove elements with size 1 from array and transforms it in string
                if len(value_parse) == 1:
                    hash_cfg[key_parse]=value_parse[0]
            return hash_cfg


    def task_run_cmd(self):
            print 'Check if yours resources can run this in parallel jobs'
            self.task_run_pooL_cmd(self.multi_task, self.task)


    def task_run_pooL_cmd(self, p_number, tasks_array):

        task_multi = Pool(p_number)
        #for x in self.task:
        #    print x , 'task_teste'
        out_log_file_name = self.path_out+'{}.log'.format(self.tool_name)
        tool_name_to_log = self.tool_name
        out_dir = self.path_out
        execution_multiparameter = partial(execute_SYSTEM_command_task, out_log_file_name, tool_name_to_log, out_dir)
        task_multi.map(execution_multiparameter, tasks_array)



class TopHat(Tool):
    def __init__(self, config, tool_name='tophat', multi_task=1):

        Tool.__init__(self, tool_name=tool_name, config=config, multi_task=multi_task)

        self.pairend = to_bool(self.config['PAIR_END'])

        self.generate_index_transcriptome = to_bool(self.config["GENERATE_TRANSCRIPTOME_INDEX"])
        print self.generate_index_transcriptome , 'gerar?', self.config["GENERATE_TRANSCRIPTOME_INDEX"]
        if self.generate_index_transcriptome == True:
            self.transcriptome_index = "--transcriptome-index " + config['PATH_CREATE_TRANSCRIPTOME_INDEX_DIR']
            self.gtf = '-G '+ self.config["PATH_GTF"]
        if self.generate_index_transcriptome == False:
            self.transcriptome_index = "--transcriptome-index " + config['PATH_INDEX_TRANSCRIPTOME'].replace('/ref_index', '')+'/ref_index'
            self.transcriptome_index = self.transcriptome_index.replace('//', '/')
            self.gtf='' #if value is equal SKIP, it Turns this value in empty value.

        self.genome_index = self.config["PATH_INDEX_GENOME"].replace('/genome', '') + '/genome'
        self.genome_index = self.genome_index.replace('//', '/')
        self.files_in_config = self.config["PATH_READS"]
        self.reads_to_use = self.search_r1_r2_pairs(self.files_in_config)
        self.generate_task(self.reads_to_use)

    def generate_out_using_name(self, name):
        if self.pairend == True:
            out_return = name.split('_R1_')[0] + "_out_tophat"
            return  out_return
        if self.pairend == False:
            name =  name.split('.')
            return "_".join(name[:-1]) + "out_tophat"

    def search_r1_r2_pairs(self, array_files):
        if self.pairend == True:
            pairs = [];
            for file in array_files:
                for pair in array_files:
                    if file != pair:
                        if file.split('_R1_')[0] == pair.split('_R2_')[0]:
                            pairs.append([file, pair])

            size_pairs = 0


            if len(pairs) == (len(array_files)/2):
                return pairs  # If all pairs are matched...
            else:
                pairs_to_locate = '\t'.join(['\t'.join(x_pairs) for x_pairs in pairs]).split('\t')
                #print pairs_to_locate
                files_error = []
                for x in array_files:
                    if x not in pairs_to_locate:
                        files_error.append(x)
                    else:
                        pass

                files_not_found_str = '\n'.join(files_error)

                raise ExperimentDesignError('\nThere some files without pairs!'
                                            ' \nRemove from directory this files or check inconsistencies in their names'
                                            ' \n{}'.format(files_not_found_str))
                sys.exit()
        if self.pairend == False:
            return array_files

    def use_created_transcriptome_index(self):
        self.transcriptome_index = "--transcriptome-index " + self.config['PATH_CREATE_TRANSCRIPTOME_INDEX_DIR'] + '/know'

    def generate_task(self, array_reads):
        if self.pairend == True:
            for R1, R2 in array_reads:
                out_dir = self.generate_out_using_name(R1)
                print out_dir
                self.out_names.append(out_dir)

                self.task.append("{path_tool} -p "
                "{threads} -o " \
                " {out_dir} " \
                " -r 150 --mate-std-dev 150 --library-type fr-secondstrand --b2-sensitive" \
                " {gtf}" \
                " {index_transcriptome}" \
                " {index_genome}" \
                " {r1}" \
                " {r2}".format(
                                path_tool=self.tool_path,
                                threads=self.threads,
                                out_dir=self.path_out + out_dir.split('/')[-1],
                                gtf=self.gtf,
                                index_transcriptome = self.transcriptome_index,
                                index_genome=self.genome_index,
                                r1=R1,
                                r2=R2

                ))

                if self.generate_index_transcriptome == True:
                    self.use_created_transcriptome_index()  # In next round use created transcriptome
                    self.gtf = '' #  Don't use gtf option
        else:
            sys.stderr.write("the unpaired function not is implemented yet" + "\n")


class CuffLinks(Tool):
    def __init__(self, config, tool_name='cufflinks', multi_task=1):
        Tool.__init__(self, tool_name=tool_name, config=config, multi_task=multi_task)
        self.gtf = self.config["PATH_GTF"]
        self.genome_fasta = self.config["PATH_FASTA_GENOME"]
        self.files_in_config = self.config["PATH_BAM"]
        self.generate_task(self.files_in_config)

    def generate_task(self, array_bams):
            for bam_file in array_bams:
                out_dir = bam_file.split('/')[-2] + "_out_cufflinks"
                self.out_names.append(out_dir)
                self.task.append('''cufflinks -p {threads} -o {out_dir} -g {gtf} -b {fasta_genome} --no-faux-reads --library-type fr-secondstrand {bam_accepteds} '''.format(
                                threads=self.threads,
                                out_dir=self.path_out + out_dir.split('/')[-1],
                                gtf=self.gtf,
                                fasta_genome=self.genome_fasta,
                                bam_accepteds= bam_file
                                )
                )


class bam_change_to_chr_tool(Tool):
    def __init__(self, config, tool_name='samtools', multi_task=1):
        Tool.__init__(self, tool_name=tool_name, config=config, multi_task=multi_task)
        self.files = self.config['PATH_FILES']
        self.generate_task(self.files)

    def generate_task(self, files):

        for bam_file in files:
            if type(bam_file) ==  list:
                out_dir = bam_file[0].split('.')[-2]
                os.system("mkdir {}{}".format(self.path_out, bam_file[1]))
                new_file_name = "with_chr_" + bam_file[0].split('/')[-1]
                self.out_names.append(out_dir)
                self.task.append('''{tool} view -h {bam} |perl -pe "s/\tSN:/\tSN:chr/g" |{tool} view -bS - > {saida}'''.format(
                                tool=self.tool_path,
                                bam=bam_file[0],
                                saida=self.path_out +bam_file[1]+ "/" + new_file_name

                                )
                )
            else:
                raise ExperimentDesignError('\nThe file need a output value.\n'
                                            'ex: PATH_FILE=/your/file/sample01_file.bam:my_sample01 )\n'
                                            'This is create a new dir with "my_sample01" name.\n{bam_file}'.format(bam_file=bam_file))
                sys.exit()

        for x in self.task:
            print x



           

def main():
    ''''''
'''
    tophat = TopHat(config={'PATH_DIR_OUT:'/work/new/tophat/'
                            PATH_TOOL':'' ,
                            'THREADS':2 ,
                            'PAIR_END':True ,
                            'GENERATE_TRANSCRIPTOME_INDEX':True ,
                            'PATH_CREATE_TRANSCRIPTOME_INDEX_DIR':'/work/lucas/' ,
                            'PATH_GTF':'/work/user/teste.gtf',
                            'PATH_INDEX_GENOME':'/work/user/human/genome',
                            'PATH_READS':['/work/teste_fastq_R1.gz','/work/teste_fastq_R2.gz','arquivos/reads_R1.gz', 'arquivos/reads_R2.gz']})
    tophat.task_run_cmd()

    '''
if __name__ == '__main__':
    sys.exit(main())

#para testar R1 e R2 não é bom pegar o path inteiro! Śo o nome do arqiuvo