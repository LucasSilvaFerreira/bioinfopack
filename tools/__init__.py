# -*- coding: utf-8 -*-
__author__ = 'lucassilva'
import sys
import os
from bioinfopack import abrir_arquivo, DirNotFound, check_program_exists, check_path_exists, AppNotFound, FileNotFound, ExperimentDesignError
import re
import logging
import datetime



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

        print 'sem path'
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
    def __init__(self, tool_name, config):
        self.config = config
        self.tool_name = tool_name
        self.check_consistence_in_config(config)

        self.threads = config["THREADS"]


        self.task = []
        self.out_names =[]
        sys.stderr.write("Esses pahts deveriam ter erros! como está indo o mecanismo de procura?" + "\n")

    def check_consistence_in_config(self, config):
        for key_file, file_path in config.iteritems():
            print key_file, file_path
            if re.match("PATH", key_file):
                if re.match("PATH_CREATE", key_file):
                    pass # checar permissoes
                elif re.match("PATH_TOOL", key_file):
                    self.tool_path = task_check_app_path(file_path, self.tool_name)

                elif re.match("PATH_INDEX", key_file):
                    pass

                else:


                    if type(file_path) == list:
                        for sub_path in file_path:
                            if re.match("PATH_DIR", key_file):
                                task_check_dir_path(sub_path)
                            else:
                                task_check_files_path(sub_path)
                    else:
                        if re.match("PATH_DIR", key_file):
                            task_check_dir_path(file_path)
                        else:
                            task_check_files_path(file_path)

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
                    key = key.lower()
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
            return hash_cfg

    def cmd_log(self, message):
        actual_time = datetime.datetime.now().strftime("%I:%M h on %B %d, %Y")
        logging.basicConfig(filename='{}.log'.format(self.tool_name), level=logging.DEBUG)
        logging.debug('\n<<{}>> Run on: {}\nCommand:\n{}'.format(self.tool_name, actual_time, message))

    def task_run_cmd(self):
        print self.task
        task_array = self.task
        for task_x in task_array:
            self.cmd_log(task_x)
            os.system(task_x)
            finished_time = datetime.datetime.now().strftime("%I:%M h on %B %d, %Y")
            logging.debug('\n<<{}>> Finished : {}'.format(self.tool_name, finished_time))

class TopHat(Tool):
    def __init__(self, config, tool_name='tophat'):


        Tool.__init__(self, tool_name=tool_name, config=config)

        self.pairend = bool(self.config['PAIR_END'])
        self.generate_index_transcriptome = bool(self.config["GENERATE_TRANSCRIPTOME_INDEX"])
        if self.generate_index_transcriptome == True:
            self.transcriptome_index = "--transcriptome-index " + config['PATH_CREATE_TRANSCRIPTOME_INDEX_DIR']
        if self.generate_index_transcriptome == False:
            self.transcriptome_index = "--transcriptome-index " + config['PATH_INDEX_TRANSCRIPTOME'].replace('/know', '')+'/know'
            self.transcriptome_index = self.transcriptome_index.replace('//', '/')
        self.gtf = self.config["PATH_GTF"]
        self.genome_index = self.config["PATH_INDEX_GENOME"].replace('/genome', '') + '/genome'
        self.genome_index = self.genome_index.replace('//', '/')
        self.files_in_config = self.config["PATH_READS"]
        self.reads_to_use = self.search_r1_r2_pairs(self.files_in_config)
        self.generate_task(self.reads_to_use)

    def generate_out_using_name(self, name):
        if self.pairend == True:
            out_return = name.split('R1')[0] + "out_tophat"
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
                        if file.split('_R1')[0] == pair.split('_R2')[0]:
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
                self.out_names.append(out_dir)

                self.task.append("{path_tool} -p "
                "{threads} -o " \
                "{out_dir} " \
                " -r 150 --mate-std-dev 150 --library-type fr-secondstrand --b2-sensitive" \
                " -G {gtf}" \
                " {index_transcriptome}" \
                " {index_genome}" \
                " {r1}" \
                " {r2}".format(
                                path_tool=self.tool_path,
                                threads=self.threads,
                                out_dir=out_dir,
                                gtf=self.gtf,
                                index_transcriptome = self.transcriptome_index,
                                index_genome=self.genome_index,
                                r1=R1,
                                r2=R2

                ))

                if self.generate_index_transcriptome == True:
                    self.use_created_transcriptome_index()  # In next round use created transcriptome
        else:
            sys.stderr.write("the unpaired function not is implemented yet" + "\n")



def main():
    ''''''

    tophat = TopHat(config={'PATH_TOOL':'' ,
                            'THREADS':2 ,
                            'PAIR_END':True ,
                            'GENERATE_TRANSCRIPTOME_INDEX':True ,
                            'PATH_CREATE_TRANSCRIPTOME_INDEX_DIR':'/work/lucas/' ,
                            'PATH_GTF':'/work/user/teste.gtf',
                            'PATH_INDEX_GENOME':'/work/user/human/genome',
                            'PATH_READS':['/work/teste_fastq_R1.gz','/work/teste_fastq_R2.gz','arquivos/reads_R1.gz', 'arquivos/reads_R2.gz']})
    tophat.task_run_cmd()
if __name__ == '__main__':
    sys.exit(main())

#para testar R1 e R2 não é bom pegar o path inteiro! Śo o nome do arqiuvo