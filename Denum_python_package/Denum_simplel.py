import glob
import random
import shutil
import tarfile
import time
from collections import defaultdict
from datetime import datetime
from collections import Counter
import pyppmd  # PPMd压缩算法的Python接口
import regex as re
import pandas as pd
import os
import copy
from itertools import zip_longest

from multiprocessing import Pool  # 用于并行处理日志块


class dataloader():
    """日志数据加载和压缩处理的主类"""

    def __init__(self,dataset_information):
        """初始化数据加载器
        
        参数:
            dataset_information: 包含日志路径和名称的字典
        """
        self.path=dataset_information['input_path']  # 日志文件路径
        self.logname=dataset_information['dataset_name']  # 日志数据集名称
        
        # 初始化存储结构
        self.logheader=[]  # 存储日志头部
        self.logcontent=[]  # 存储日志内容
        self.LZMA_list=[]  # LZMA压缩内容列表
        self.PPMD_list=[]  # PPMd压缩内容列表

    # 似乎没用该函数？
    def load_data(self,chunkID):
        """加载特定块的日志数据
        
        参数:
            chunkID: 日志块ID
        """
        headers, regex = self.generate_logformat_regex(self.logformat)
        self.logheader,self.logcontent = self.log_to_dataframe(self.path, regex, headers,self.caldelta,chunkID)

    def generate_logformat_regex(self, logformat):
        """根据日志格式生成正则表达式以分割日志消息
        
        参数:
            logformat: 日志格式字符串
        返回:
            headers: 头部字段列表
            regex: 编译好的正则表达式
        """
        headers = []
        splitters = re.split(r'(<[^<>]+>)', logformat)
        regex = ''
        for k in range(len(splitters)):
            if k % 2 == 0:
                splitter = re.sub(' ', '\\\s+', splitters[k])
                regex += splitter
            else:
                header = splitters[k].strip('<').strip('>')
                if header in self.digit_headers:
                    regex += '(?P<%s>\d+)' % header
                else:
                    regex += '(?P<%s>.*?)' % header
                headers.append(header)
        regex = re.compile('^' + regex + '$')
        return headers, regex

    def log_to_dataframe(self, log_file, regex, headers, caldelta, chunkID):
        """将日志文件转换为结构化数据
        
        参数:
            log_file: 日志文件路径
            regex: 用于解析的正则表达式
            headers: 头部字段列表
            caldelta: 需要计算增量的时间戳字段
            chunkID: 块ID
        返回:
            log_headers: 处理后的日志头部
            log_content: 处理后的日志内容
        """
        log_headers=[]
        log_content=[]
        linecount = 0
        delta_temp=defaultdict(list)  # 存储时间戳增量
        last_num=defaultdict(int)  # 记录上一个时间戳值

        # 逐行处理日志文件
        with open(log_file, 'r', encoding="ISO-8859-1") as fil:
            for line in fil.readlines():
                    linecount += 1
                    header_list = ''
                    try:
                        # 使用正则表达式解析日志行
                        match = re.search(regex, line, timeout=0.5)
                        for header in headers:
                            # 处理时间戳字段，计算增量
                            if header in caldelta:
                                matched_time=match.group(header)
                                timenumbers = re.findall(r'\d+', matched_time)
                                if header not in last_num.keys():
                                    # 第一个时间戳值特殊处理
                                    last_num[header]=int(''.join(timenumbers))
                                    modified_line = re.sub(r'\d+', '<*>', matched_time)
                                    with open('../Output/'+self.logname+'/'+str(chunkID)+'/PPMd/'+caldelta+'.txt','w', encoding="ISO-8859-1") as file:
                                        file.write(modified_line+'\n'+str(''.join(timenumbers)))
                                else:
                                    # 计算与前一个值的差值并存储
                                    timenumbers=''.join(timenumbers)
                                    delta=int(timenumbers)-int(last_num[header])
                                    last_num[header]=int(timenumbers)
                                    delta_temp[header].append(delta)
                                header_list = header_list+"<"+header+">"
                                continue
                            if header != 'Content':
                                header_list=header_list+(match.group(header))
                        log_content.append(match.group('Content'))
                        log_headers.append(header_list)
                    except Exception as e:
                        # 解析失败，保存原始行
                        log_headers.append('')
                        log_content.append(line.strip())
                        pass
            
            # 存储时间戳增量
            for key in delta_temp:
                self.store_content_with_ids(delta_temp[key], key, type='number', chunkID=chunkID)

        # 处理头部和内容中的数字
        log_headers,num1 = self.replace_numbers_and_save(log_headers,chunkID,'header')
        log_content,num2 = self.replace_numbers_and_save(log_content,chunkID,'content')
        num1.extend(num2)
        
        # 存储所有提取的数字
        ids_file_path = '../Output/' + self.logname + '/' + str(chunkID) + '/lzma/' + self.logname+ 'nums.bin'
        with open(ids_file_path, 'ab') as ids_file:
            for id in num1:
                ids_file.write(elastic_encoder(int(id)))  # 使用弹性编码存储数字
        return log_headers,log_content

    def store_content_with_ids(self,input, output, type, chunkID, compressor):
        """使用ID存储内容，实现字典编码
        
        参数:
            input: 输入内容列表
            output: 输出文件名前缀
            type: 数据类型('number'或'str')
            chunkID: 块ID
            compressor: 使用的压缩器类型
        """
        content_to_id = {}  # 内容到ID的映射
        id_to_content = {}  # ID到内容的映射
        id_counter = 1  # ID计数器，从1开始
        
        # 设置文件路径
        ids_file_path = '../Output/'+self.logname+'/'+str(chunkID)+'/lzma/'+self.logname+str(output)+'ids.bin'
        if type=='number':
            mapping_file_path = '../Output/' + self.logname + '/' + str(chunkID) + '/lzma/' + self.logname + str(
                output) + 'mapping.bin'
        else:
            mapping_file_path = '../Output/'+self.logname+'/'+str(chunkID)+'/'+compressor+'/'+self.logname+str(output)+'mapping.txt'
        
        id_list = []
        # 为每个内容分配唯一ID
        for line in input:
                if isinstance(line,str):
                    line = line.strip()
                if line != None:
                    if line not in content_to_id:
                        # 新内容，分配新ID
                        content_id = id_counter
                        content_to_id[line] = content_id
                        id_to_content[content_id] = line
                        id_counter += 1
                    else:
                        # 已存在的内容，使用已分配的ID
                        content_id = content_to_id[line]
                    id_list.append(content_id)
                else:
                    id_list.append(0)
                    
        # 根据类型不同，采用不同的存储方式
        if type=='number':
            # 数字类型：使用二进制存储
            with open(ids_file_path, 'ab') as ids_file, \
                        open(mapping_file_path, 'ab') as mapping_file:
                    for content, content_id in content_to_id.items():
                        mapping_file.write(elastic_encoder(int(content)))
                    for id in id_list:
                        ids_file.write(elastic_encoder(id))
        else:
            # 字符串类型：文本存储
            with open(ids_file_path, 'ab') as ids_file, \
                    open(mapping_file_path, 'a', encoding="ISO-8859-1") as mapping_file:
                for content, content_id in content_to_id.items():
                    mapping_file.write(f'{content}\n')
                for id in id_list:
                    ids_file.write(elastic_encoder(id))

    def store_numlist_with_ids(self,input, output, type, chunkID):
        """存储数字列表，为数字序列分配ID
        
        参数:
            input: 输入数字列表
            output: 输出文件名前缀
            type: 数据类型
            chunkID: 块ID
        """
        content_to_id = {}
        id_to_content = {}
        id_counter = 1
        
        # 设置文件路径
        ids_file_path = '../Output/'+self.logname+'/'+str(chunkID)+'/lzma/'+self.logname+str(output)+'ids.bin'
        if type=='number':
            mapping_file_path = '../Output/' + self.logname + '/' + str(chunkID) + '/lzma/' + self.logname + str(
                output) + 'mapping.bin'
        else:
            mapping_file_path = '../Output/'+self.logname+'/'+str(chunkID)+'/PPMd/'+self.logname+str(output)+'mapping.txt'
        
        id_list = []
        # 处理数字列表并分配ID
        for line in input:
                line=' '.join(line)
                if isinstance(line,str):
                    line = line.strip()
                if line != None:
                    if line not in content_to_id:
                        content_id = id_counter
                        content_to_id[line] = content_id
                        id_to_content[content_id] = line
                        id_counter += 1
                    else:
                        content_id = content_to_id[line]
                    id_list.append(content_id)
                else:
                    id_list.append(0)
        
        # 根据类型不同，采用不同的存储方式
        if type=='number':
            with open(ids_file_path, 'ab') as ids_file, \
                        open(mapping_file_path, 'ab') as mapping_file:
                    for content, content_id in content_to_id.items():
                        mapping_file.write(elastic_encoder(int(content)))
                    for id in id_list:
                        ids_file.write(elastic_encoder(id))
        else:
            with open(ids_file_path, 'ab') as ids_file, \
                    open(mapping_file_path, 'a', encoding="ISO-8859-1") as mapping_file:
                for content, content_id in content_to_id.items():
                    mapping_file.write(f'{content}\n')
                for id in id_list:
                    ids_file.write(elastic_encoder(id))

    def kernel_compress(self,chunkID):
        """执行最终压缩，使用多种压缩算法组合
        
        参数:
            chunkID: 块ID
        返回:
            totalsize: 压缩后总大小
        """
        # 1. 创建PPMd目录的tar包
        create_tar('../Output/'+self.logname+'/'+str(chunkID)+'/PPMd','../Output/'+self.logname+'/'+str(chunkID)+'/PPMd/temp.tar')
        
        # 2. 使用PPMd算法压缩tar包
        compress_tar_with_ppmd('../Output/'+self.logname+'/'+str(chunkID)+'/PPMd/temp.tar', '../Output/'+self.logname+'/'+str(chunkID)+'/temp.ppmd')
        achieved_size_ppmd = get_file_size('../Output/'+self.logname+'/'+str(chunkID)+'/temp.ppmd')
        
        # 3. 使用LZMA压缩结构化数据
        General7z_compress_lzma('../Output/'+self.logname+'/'+str(chunkID)+'/lzma')
        achieved_size_lzma = get_file_size('../Output/'+self.logname+'/'+str(chunkID)+'/lzma/'+'temp.tar.'+'xz')

        # 4. 使用bzip2压缩额外数据
        General7z_compress_bzip2('../Output/' + self.logname + '/' + str(chunkID) + '/bzip2')
        achieved_size_bzip2 = get_file_size(
            '../Output/' + self.logname + '/' + str(chunkID) + '/bzip2/' + 'temp.tar.' + 'bz2')

        # 计算总压缩大小
        totalsize=achieved_size_lzma+achieved_size_bzip2
        print('achieved size = '+str(totalsize))

        return totalsize

    def kernel_decompress(self,chunkID,type):
        """解压缩指定块的数据
        
        参数:
            chunkID: 块ID
            type: 压缩类型
        """
        file_path='../Output/'+self.logname+'/'+str(chunkID)+'/lzma/temp.tar.'+type
        decompress_path='../decompress_output/'+self.logname+'/'+str(chunkID)
        create_and_empty_directory(decompress_path)  # 创建并清空解压目录
        if type=='xz':
            with tarfile.open(file_path) as tar:
                tar.extractall(path=decompress_path)  # 解压tarfile

    def process_chunk(self,chunkID, chunk_data, logname):
        """处理单个日志块，完成完整压缩流程
        
        参数:
            chunkID: 块ID
            chunk_data: 块数据内容
            logname: 日志名称
        返回:
            achieved_size: 压缩后大小
        """
        # 1. 创建输出目录
        create_and_empty_directory('../Output/' + logname + '/' + str(chunkID) + '/PPMd')
        create_and_empty_directory('../Output/' + logname + '/' + str(chunkID) + '/lzma')
        create_and_empty_directory('../Output/' + logname + '/' + str(chunkID) + '/bzip2')
        
        # 2. 数字模式识别和替换
        Denum_logs = self.replace_numbers_and_save_by_order_binary(chunk_data, chunkID, 'all')
        
        # 3. 变量提取
        Denum_logs = self.variable_extract(Denum_logs, chunkID)
        
        # 4. 存储处理后的日志模板
        self.store_content_with_ids(Denum_logs, "all", type='str', chunkID=chunkID, compressor='lzma')
        
        # 5. 执行最终压缩
        achieved_size = self.kernel_compress(chunkID)
        return achieved_size

    def compress(self):
        """完整的日志压缩流程，包括分块处理和并行计算
        """
        chunkID = 1
        size_total = 0
        achieved_size = 0
        time0 = time.perf_counter()  # 记录开始时间

        # 加载日志文件
        # log_file_path = 'C:/Users/19449/denum/Denum/Logs/' + self.logname + '/' + self.logname + '.log'
        log_file_path = self.path
        if not os.path.exists(log_file_path):
            print(f'日志文件 {log_file_path} 不存在。')
            return

        # 读取全部日志
        with open(log_file_path, 'r', encoding="ISO-8859-1") as fil:
            print("what")
            logs = fil.readlines()
        
        # 将日志分成每100000行一个块
        chunks = [logs[i:i + 100000] for i in range(0, len(logs), 100000)]

        # 使用多进程并行处理各个块
        with Pool(processes=4) as pool:
            results = pool.starmap(self.process_chunk, [(i + 1, chunk, self.logname) for i, chunk in enumerate(chunks)])

        # 累计所有块的压缩结果
        for achieved_size_chunk in results:
            achieved_size += achieved_size_chunk

        # 计算压缩比和性能指标
        size_total = get_file_size(log_file_path)
        compression_ratio = size_total / achieved_size
        print('压缩比 = ' + str(compression_ratio))
        
        time1 = time.perf_counter()
        timecost = time1 - time0
        CS = size_total / (1024 * 1024) / timecost
        print('压缩速度 = ' + str(CS) + ' MB/S')

    def replace_numbers_and_save_by_order_binary(self, input_file, chunkID, type):
        """替换日志中的数字模式并按顺序保存到二进制文件
        
        参数:
            input_file: 输入文件内容
            chunkID: 块ID
            type: 处理类型
        返回:
            modified_lines: 处理后的行
        """
        # 识别和替换数字模式
        grouped, dict = self.replace_and_group(input_file)
        modified_lines = grouped

        # 对每种数字模式分别处理
        for key in dict:
            label = key[1:-1]  # 去掉尖括号，获取模式标签
            
            # 存储到二进制文件
            ids_file_path = '../Output/' + self.logname + '/' + str(chunkID) + '/lzma/' + "_" + label + '_.bin'
            with open(ids_file_path, 'ab') as ids_file:
                    if label == 'N' or label == 'I':
                        # IP地址和一般数字直接存储
                        for id in dict[key]:
                            ids_file.write(elastic_encoder(int(id)))
                    else:
                        # 时间戳等应用增量编码
                        save = delta_transform(dict[key])
                        for id in save:
                            ids_file.write(elastic_encoder(int(id)))

        return modified_lines
    
    # def delimeter_mining(self,logs):
    #     temp=logs.copy()
    #     random.shuffle(temp)
    #     lenth=[]
    #     sample=[]
    #     for log in temp:
    #         if len(log) not in lenth:
    #             lenth.append(len(log))
    #             sample.append(log)
    #         else:
    #             continue
    #         if len(lenth)>=10:
    #             break
    #     delimeters=self.find_special_chars_with_high_freq(sample)
    #
    #     return delimeters
    #
    # def find_special_chars_with_high_freq(self,str_list, freq_threshold=10):
    #     candidate=[',',' ','|',';','[',']','(',')']
    #     char_counter = Counter()
    #     for s in str_list:
    #         for char in s.strip():
    #             if char in candidate:
    #                 char_counter[char] += 1
    #
    #     result = [char for char, count in char_counter.items() if count > freq_threshold]
    #
    #     return result
    #
    # def delimeter_mining(self, logs):
    #     temp = logs.copy()
    #     random.shuffle(temp)
    #     lenth = set()
    #     sample = []
    #     for log in temp:
    #         log_len = len(log)
    #         if log_len not in lenth:
    #             lenth.add(log_len)
    #             sample.append(log)
    #         if len(lenth) >= 10:
    #             break
    #     delimiters = self.find_special_chars_with_high_freq(sample)
    #
    #     return delimiters

    def find_special_chars_with_high_freq(self, str_list, freq_threshold=10):
        """查找高频特殊字符作为可能的分隔符
        
        参数:
            str_list: 字符串列表
            freq_threshold: 频率阈值
        返回:
            result: 高频特殊字符列表
        """
        candidates = [',', ' ', '|', ';', '[', ']', '(', ')']  # 候选分隔符
        char_counter = Counter()
        
        # 统计每个特殊字符的出现频率
        for s in str_list:
            stripped_s = s.strip()
            filtered_chars = [char for char in stripped_s if char in candidates]
            char_counter.update(filtered_chars)

        # 返回频率超过阈值的字符
        result = [char for char, count in char_counter.items() if count > freq_threshold]
        return result
    
    def store_processed_logs(self, logs, chunkID):
        """处理并存储日志结构模板
        
        参数:
            logs: 日志内容
            chunkID: 块ID
        """
        ids_file_path = '../Output/' + self.logname + '/' + str(
            chunkID) + '/lzma/processed' + self.logname + 'template.bin'
        dict = defaultdict(list)
        id_dict = {}
        v_id = []
        id = 0
        
        # 提取分隔符
        delimeter = self.delimeter_mining(logs)
        
        # 处理每行日志
        for log in logs:
            split = self.split_by_multiple_delimiters(delimeter, log.strip())
            lenth = len(split)
            # 计算特殊字符的模式标签
            tag = self.count_special_characters(delimeter, log, lenth)
            if tag not in id_dict:
                id_dict[tag] = id
                id += 1
            label = id_dict[tag]
            dict[tag].append(split)
            v_id.append(label)
            
        # 存储模板ID序列
        with open(ids_file_path, 'wb') as file:
            for id in v_id:
                file.write(elastic_encoder(int(id)))
                
        # 保存模板结构
        self.split_and_save_template(dict, chunkID)

    def variable_extract(self, logs, chunkID):
        """从日志中提取变量
        
        参数:
            logs: 日志内容
            chunkID: 块ID
        返回:
            modified_lines: 处理后的行
        """
        varibale_set = []  # 变量集合
        modified_lines = []  # 修改后的日志行
        digit_pattern = re.compile(r'\d')  # 数字模式正则表达式
        
        # 提取分隔符
        regex_pattern, delimiters = self.delimeter_mining(logs)
        
        # 处理每行日志
        for log in logs:
            modified_line = ''
            # 使用分隔符分割日志行
            split = self.split_by_multiple_delimiters(regex_pattern, log.strip())
            
            # 对每个词进行处理
            for word in split:
                if digit_pattern.search(word):  # 如果包含数字，认为是变量
                    modified_line = modified_line + '<*>'  # 替换为变量占位符
                    varibale_set.append(word)  # 保存原始变量值
                else:
                    modified_line = modified_line + word  # 保留非变量部分
            modified_lines.append(modified_line)
            
        # 存储变量集合
        self.store_content_with_ids(varibale_set, 'variableset', 'str', chunkID, compressor='lzma')
        return modified_lines

    def delimeter_mining(self, logs):
        """挖掘日志中的分隔符
        
        参数:
            logs: 日志内容
        返回:
            regex_pattern: 分隔符的正则表达式
            delimiters: 分隔符列表
        """
        temp = logs.copy()
        random.shuffle(temp)  # 随机打乱日志行
        lengths = set()
        sample = []
        
        # 从不同长度的日志中采样
        for log in temp:
            log_len = len(log)
            if log_len not in lengths:
                lengths.add(log_len)
                sample.append(log)
            if len(lengths) >= 10:
                break
                
        # 查找高频特殊字符作为分隔符
        delimiters = self.find_special_chars_with_high_freq(sample)
        
        # 创建分隔符正则表达式
        regex_pattern = re.compile('(' + '|'.join(re.escape(delimiter) for delimiter in delimiters) + ')')
        return regex_pattern, delimiters

    def split_by_multiple_delimiters(self, regex_pattern, string_to_split):
        """使用多个分隔符分割字符串
        
        参数:
            regex_pattern: 分隔符正则表达式
            string_to_split: 要分割的字符串
        返回:
            分割后的部分列表
        """
        return regex_pattern.split(string_to_split)

    def count_special_characters(self, a, b, lenth):
        """计算特殊字符的出现模式
        
        参数:
            a: 特殊字符列表
            b: 要检查的字符串
            lenth: 长度限制
        返回:
            result: 表示特殊字符出现次数的字符串
        """
        result = ""
        for char in a:
            count = b.count(char)
            if count > 0:
                result += str(count) + char
        return result

    def split_and_save_template(self, dict, chunkID):
        """分割并保存模板
        
        参数:
            dict: 模板字典
            chunkID: 块ID
        """
        template_list = []
        template_id = 0
        var_dict = {}
        var_id = 0
        
        # 处理每组模板
        for key in dict:
            sub_id = 0
            template = ''
            # 按位置分组相同位置的词
            group = self.split_and_group_by_space(dict[key])
            
            # 检查每个位置是否为常量或变量
            for lis in group:
                if len(set(lis)) == 1:  # 如果该位置所有值相同，是常量
                    template = template + str(lis[0])
                else:  # 如果有不同值，是变量
                    template = template + '<*>'
                    # 存储该位置的所有变量值
                    var_dict, var_id = self.store_variable(lis, var_dict, var_id, str(template_id) + '-' + str(sub_id), chunkID)
                sub_id += 1
            template_list.append(template)
            template_id += 1
            
        # 保存变量字典和模板列表
        self.store_file(var_dict.keys(), chunkID, 'var_dict', 'lzma')
        self.store_file(template_list, chunkID, 'template', 'lzma')

    def store_variable(self, var_list, var_dict, var_id, tag, chunkID):
        """存储变量及其ID映射
        
        参数:
            var_list: 变量列表
            var_dict: 变量字典
            var_id: 变量ID计数器
            tag: 标签
            chunkID: 块ID
        返回:
            var_dict: 更新后的变量字典
            var_id: 更新后的ID计数器
        """
        id_list = []
        # 为每个变量分配唯一ID
        for var in var_list:
            if var not in var_dict:
                var_dict[var] = var_id
                var_id += 1
            id_list.append(var_dict[var])
            
        # 存储ID序列
        ids_file_path = '../Output/' + self.logname + '/' + str(
            chunkID) + '/lzma/processed' + self.logname + tag + 'ids.bin'
        with open(ids_file_path, 'wb') as file:
            for id in id_list:
                file.write(elastic_encoder(id))
        return var_dict, var_id

    def store_file(self, list, chunkID, tag, compressor):
        """存储内容到文件
        
        参数:
            list: 内容列表
            chunkID: 块ID
            tag: 标签
            compressor: 压缩器类型
        """
        dict_file_path = '../Output/' + self.logname + '/' + str(
            chunkID) + '/' + compressor + '/processed' + self.logname + str(tag) + '.txt'
        with open(dict_file_path, 'w', encoding="ISO-8859-1") as file:
            for line in list:
                file.write(line)
                file.write('\n')

    def split_and_group_by_space(self, input_list):
        """按空格分割并分组
        
        参数:
            input_list: 输入列表
        返回:
            grouped: 分组后的列表
        """
        grouped = list(zip(*(s for s in input_list)))
        grouped = [list(group) for group in grouped]
        return grouped

    # def replace_and_group(self,lst):
    #     patterns = defaultdict(list)
    #     replaced = []
    #     # Define a replacement function
    #     def find_timestamp_and_combine(match):
    #         num = match.group()
    #         numbers = re.findall(r'\d+', num)
    #         combined_number = int(''.join(numbers)) # 将三个数字连接起来  # 转换为整数并添加到列表中
    #         patterns['<T>'].append(int(combined_number))
    #         return '<T>'
    #     def find_timestamp_and_combine2(match):
    #         num = match.group()
    #         numbers = re.findall(r'\d+', num)
    #         combined_number = int(''.join(numbers)) # 将三个数字连接起来  # 转换为整数并添加到列表中
    #         patterns['<TT>'].append(int(combined_number))
    #         return '<TT>'
    #     def find_timestamp_and_combine3(match):
    #         num = match.group()
    #         numbers = re.findall(r'\d+', num)
    #         combined_number = int(''.join(numbers)) # 将三个数字连接起来  # 转换为整数并添加到列表中
    #         patterns['<TTT>'].append(int(combined_number))
    #         return '<TTT>'
    #     def find_IP_and_combine(match):
    #         num = match.group()
    #         numbers = re.findall(r'\d+', num)
    #         combined_number = int(''.join(numbers)) # 将三个数字连接起来  # 转换为整数并添加到列表中
    #         patterns['<I>'].append(int(combined_number))
    #         return '<I>'
    #     def replace_with_pattern(match):
    #         num = match.group()
    #         alpha=['a','b','c','d','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
    #         pattern = f"<{len(num)}N{num[0]}>"
    #
    #         if len(num)==1 :
    #             pattern = f"<{alpha[len(num)]}>"
    #             patterns[pattern].append(num)
    #             return pattern
    #
    #         if len(num)==3 and num[0]=='0':
    #             pattern = f"<{alpha[len(num)]}>"
    #             patterns[pattern].append(num)
    #             return pattern
    #
    #         if len(num)>=15:
    #             return num
    #
    #         if len(num)>=4:
    #             pattern = f"<{alpha[len(num)]}{alpha[int(num[0])]}>"
    #             patterns[pattern].append(num)
    #             return pattern
    #
    #         else:
    #             patterns['<*>'].append(num)
    #             return '<n>'
    #
    #     for item in lst:
    #
    #         if self.logname=='BGL':
    #             replaced_item = re.sub(r'(\d{4})\.(\d{2})\.(\d{2})', find_IP_and_combine, item)
    #             replaced_item = re.sub(r'(\d{4})-(\d{2})-(\d{2})', find_timestamp_and_combine2, replaced_item)
    #             replaced_item = re.sub(r'(\d{2})\.(\d{2})\.(\d{2})', find_timestamp_and_combine, replaced_item)
    #             replaced_item = re.sub(r'(?<![a-zA-Z0-9])\d+(?![a-zA-Z0-9])', replace_with_pattern, replaced_item)
    #         else:
    #             replaced_item = re.sub(r'(\d+)\.(\d+)\.(\d+)\.(\d+)', find_IP_and_combine, item)
    #             replaced_item = re.sub(r'(\d+):(\d+):(\d+)\.(\d+)', find_timestamp_and_combine2, replaced_item)
    #             replaced_item = re.sub(r'(\d+):(\d+):(\d+)', find_timestamp_and_combine, replaced_item)
    #             replaced_item = re.sub(r'(?<![a-zA-Z0-9])\d+(?![a-zA-Z0-9])', replace_with_pattern, replaced_item)
    #         replaced.append(replaced_item)
    #
    #     # for key in patterns:
    #     #     if '<4N' in key:
    #     #         print(key)
    #     #         print(len(set(patterns[key])))
    #
    #     return replaced, dict(patterns)

    def replace_and_group(self, lst):
        """识别并替换日志中的各类数字模式，并按模式分组存储
        
        这是 Denum 压缩系统的核心方法之一，负责识别日志中的数值模式，
        将它们替换为特定标记，并按类型分组保存，用于后续压缩。
        
        参数:
            lst: 输入日志行列表
        
        返回:
            replaced: 替换数值后的日志行列表
            dict(patterns): 按模式分类的原始数值字典
        """
        patterns = defaultdict(list)  # 用于存储不同模式的数字，按模式类型分组
        replaced = []  # 存储替换后的日志行
        
        # 定义各种数字模式的正则表达式
        ip_pattern = re.compile(r'(\d+)\.(\d+)\.(\d+)\.(\d+)')  # IP地址: 192.168.1.1
        timestamp_pattern_2 = re.compile(r'(\d+):(\d+):(\d+)\.(\d+)')  # 带毫秒的时间戳: 12:34:56.789
        timestamp_pattern_4 = re.compile(r'(\d+)-(\d+)-(\d+)-(\d+)\.(\d+)\.(\d+)')  # 复杂日期时间格式： 2023-10-01-12.34.56【问题：BGL的秒小数部分呢？答：num_pattern单独处理了】
        timestamp_pattern_3 = re.compile(r'(\d+):(\d+)')  # 简单时间: 12:34
        float_pattern = re.compile(r'(\d+)\.(\d+)')  # 浮点数: 3.14
        timestamp_pattern_1 = re.compile(r'(\d+):(\d+):(\d+)')  # 标准时间: 12:34:56
        num_pattern = re.compile(r'(?<![a-zA-Z0-9])\d+(?![a-zA-Z0-9])')  # 独立数字（即不包含字符的token）
        # TODO: 这个正则表达式不太理解
        alpha = 'abcdefghijklmnopqrstuvwxyz'  # 字母表，用于编码不同长度的数字

        def find_and_combine(pattern_key, match):
            """处理 时间戳格式 的数字序列
            
            将多个数字组合成一个大数字，并保存在对应模式下
            
            参数:
                pattern_key: 模式标识符，如 <T> 或 <TT>
                match: 正则匹配结果
                
            返回:
                模式标识符，用于替换原始文本
            """
            numbers = re.findall(r'\d+', match.group())  # 提取所有数字
            combined_number = int(''.join(numbers))  # 将多个数字连接成一个整数
            patterns[pattern_key].append(combined_number)  # 存入对应模式组
            return pattern_key  # 返回模式标识符，替换原文本

        def find_and_combineIP(pattern_key,  match):
            """特殊处理IP地址格式
        
            为IP地址的每个段补零，确保存储格式统一
            
            参数:
                pattern_key: 模式标识符，通常为 <I>
                match: 正则匹配结果
                
            返回:
                模式标识符，用于替换原始文本
            """
            # 找出所有的数字
            numbers = re.findall(r'\d+', match.group())
            # 确保每个数字都是三位数，不足三位在前面补零
            padded_numbers = [num.zfill(3) for num in numbers]
            # 将这些数字组合成一个长数字
            combined_number = int(''.join(padded_numbers))
            # 将组合好的数字加入到对应的键下的列表中
            patterns[pattern_key].append(combined_number)
            return pattern_key

        def replace_with_pattern(match):
            """处理一般数字，根据长度和首位数字选择不同编码模式
            
            对不同长度的数字使用不同的占位符，优化压缩效果
            
            参数:
                match: 正则匹配结果
                
            返回:
                替换后的模式标记
            """
            num = match.group()  # 获取匹配到的数字
            
            # 根据数字长度确定模式标签
            if len(num) == 2:  # 两位数使用 <a>
                pattern = f"<{alpha[len(num)-1]}>"
            elif len(num) == 3:  # 三位数使用 <b>
                pattern = f"<{alpha[len(num)-1]}>"
            elif len(num) >= 15:  # 超长数字直接保留，不进行替换
                return num
            elif len(num) >= 4:  # 长数字使用 <长度首位数> 模式，如 <dc> 表示4位且首数字为3的数
                pattern = f"<{alpha[len(num)-1]}{alpha[int(num[0])-1]}>"
            else:  # 其他情况
                pattern = f"<{alpha[len(num)-1]}>"
                
            # 存储原始数字值
            patterns[pattern].append(num)
            return pattern  # 返回替换模式

        # 处理每行日志
        for item in lst:
            if self.logname == 'BGL':  # BGL日志格式特殊处理
                # BGL日志使用不同的模式替换顺序
                replaced_item = timestamp_pattern_4.sub(lambda m: find_and_combine('<TT>', m), item)
                replaced_item = timestamp_pattern_1.sub(lambda m: find_and_combine('<T>', m), replaced_item)
                replaced_item = num_pattern.sub(replace_with_pattern, replaced_item)
            else:  # 常规日志处理
                # 按照匹配优先级顺序依次替换
                replaced_item = ip_pattern.sub(lambda m: find_and_combineIP('<I>', m), item)
                replaced_item = timestamp_pattern_2.sub(lambda m: find_and_combine('<TT>', m), replaced_item)
                replaced_item = timestamp_pattern_1.sub(lambda m: find_and_combine('<T>', m), replaced_item)
                replaced_item = timestamp_pattern_3.sub(lambda m: find_and_combine('<TT>', m), replaced_item)
                replaced_item = num_pattern.sub(replace_with_pattern, replaced_item)
                
            replaced.append(replaced_item)  # 保存替换后的行

        return replaced, dict(patterns)  # 返回替换结果和模式字典

    def replace_numbers_and_save(self, input_file, chunkID, type):
        """替换日志中的数字并按模式分类保存
        
        该方法识别日志中的数字，将其替换为占位符，然后将相同模式的数字分组存储，
        以便于后续的压缩处理。
        
        参数:
            input_file: 输入的日志行列表
            chunkID: 数据块的ID，用于文件命名
            type: 处理类型标识符，如'header'或'content'
        
        返回:
            modified_lines: 替换数字后的日志行列表
        """
        numbers_collection = {}  # 存储不同模式的数字集合
        modified_lines = []      # 存储替换后的日志行
        num_dict = {}            # 数字模式到ID的映射
        num_ID = 0               # 模式ID计数器
        
        # 处理每行日志
        for line in input_file:
            # 提取当前行中的所有数字
            current_numbers = re.findall(r'\d+', line)
            # 将所有数字替换为占位符 <*>
            modified_line = re.sub(r'\d+', '<*>', line)
            
            # 查找包含占位符的模式，如 "abc<*>def<*>ghi"
            pattern = r'\B\S*<\*>\S*\B'
            matches = re.findall(pattern, modified_line)
            
            # 处理每个匹配的模式
            for i in matches:
                # 如果是新的模式，初始化并分配ID
                if i not in numbers_collection:
                    numbers_collection[i] = []
                    num_dict[i] = num_ID
                    num_ID += 1
                    
                # 计算当前模式中占位符的数量
                placeholder_num = i.count('<*>')
                # 从current_numbers中取出对应数量的数字
                save_nums = current_numbers[:placeholder_num]
                # 更新current_numbers，移除已处理的数字
                current_numbers = current_numbers[placeholder_num:]
                # 将数字列表添加到对应模式的集合中
                numbers_collection[i].append(save_nums)
                
            # 保存处理后的行
            modified_lines.append(modified_line)
        
        # 保存所有识别出的数字模式到文件
        with open('../Output/' + self.logname + '/' + str(chunkID) + '/lzma/numformat.txt', 'w',
                encoding="ISO-8859-1") as file:
            for key in numbers_collection.keys():
                file.write(key)
                file.write('\n')
        
        # 处理并存储每种数字模式
        count = 0
        for key in numbers_collection.keys():
            num_list = numbers_collection[key]
            
            # 特殊处理空模式
            if key == '<>':
                self.store_numlist_with_ids(num_list, str(count), '0', chunkID)
                count += 1
            else:
                # 转置列表，按位置分组
                # 例如: [["1","2"],["3","4"]] -> [["1","3"],["2","4"]]
                # 这样可以将相同位置的数字存储在一起，提高压缩率
                num_list = list(map(list, zip(*num_list)))
                sub = 0
                
                # 为每个位置的数字序列创建单独的二进制文件
                for nl in num_list:
                    with open('../Output/' + self.logname + '/' + str(chunkID) + '/lzma/' + 
                            str(count) + '_' + str(sub) + '.bin', 'wb') as file:
                        # 使用弹性编码存储每个数字
                        for num in nl:
                            file.write(elastic_encoder(int(num)))
                    sub += 1
                count += 1
        
        # 以下是注释掉的旧代码，保留作为参考
        # 特殊处理时间戳
        # if key == '<*>:<*>:<*>':
        #     num_list = numbers_collection[key]
        #     num_list = delta_transform(num_list)  # 增量编码
        #     with open('../Output/' + self.logname + '/' + str(chunkID) + '/lzma/'+str(count)+ '.bin','wb') as file:
        #         for num in num_list:
        #             file.write(elastic_encoder(num))
        #     count+=1
        # else:
        #     num_list = numbers_collection[key]
        #     with open('../Output/' + self.logname + '/' + str(chunkID) + '/lzma/'+str(count)+ '.txt','w',encoding="ISO-8859-1") as file:
        #         for num in num_list:
        #             file.write(num)
        #             file.write('\n')
        #     count+=1
        
        # 返回替换了数字的日志行
        return modified_lines


    def split_and_store(self, inputlist, tag, chunkID):
        """将日志按空格分割成多个部分并分别存储
        
        该方法用于将已处理的日志进行结构化分割，并分别存储不同的部分，
        这样可以更有效地压缩具有类似结构的日志内容。
        
        参数:
            inputlist: 输入的日志行列表
            tag: 标记，用于文件命名
            chunkID: 数据块的ID，用于文件命名
        """
        lines = inputlist
        
        # 计算所有非空日志行中，按空格分割后的最少块数，加1作为最小分块数
        min_blocks = min(len(line.split()) for line in lines if line.strip()) + 1
        
        # 创建分组字典，每个位置的词单独存储
        adict = defaultdict(list)
        
        # 处理每行日志
        for line in lines:
            if not line.strip():  # 跳过空行
                continue
                
            # 按空格分割日志行
            parts = line.split()
            
            # 将前 min_blocks-1 个部分单独存储
            for i in range(min_blocks - 1):
                adict[i].append(parts[i])
            
            # 将剩余部分合并为一个字符串存储
            # 这确保了不同长度的日志行也能被正确处理
            adict[min_blocks].append(' '.join(parts[min_blocks - 1:]))
        
        # 分别存储每个位置的内容
        num = 0
        for key in adict:
            temp = adict[key]
            # 为每个位置的内容调用存储函数，使用递增的标识符
            self.store_content_with_ids(temp, tag + str(num), type='str', chunkID=chunkID)
            num += 1

    def decompress(self):
        chunkID=1
        while True:
            template_list=[]
            chunk_file_path = '../Output/'+self.logname+'/'+str(chunkID)
            if os.path.exists(chunk_file_path):
                self.kernel_decompress(chunkID=chunkID,type='xz')
                print("1")
            else:
                break

            template_path='../decompress_output/'+self.logname+'/'+str(chunkID)+'/'+self.logname+'allmapping.txt'
            variable_path='../decompress_output/'+self.logname+'/'+str(chunkID)+'/'+self.logname+'variablesetmapping.txt'
            variable_id_path = '../decompress_output/' + self.logname + '/' + str(
                chunkID) + '/' + self.logname + 'variablesetids.bin'
            template_id_path='../decompress_output/'+self.logname+'/'+str(chunkID)+'/'+self.logname+'allids.bin'
            out_put_path='../decompress_output/'+self.logname+'/'+str(chunkID)+'/Decompressed'+self.logname+'.log'
            directory_to_search = '../decompress_output/'+self.logname+'/'+str(chunkID)+'/'
            pattern = '_*_.bin'
            matching_files = self.find_files(directory_to_search, pattern)

            with open(template_path,'r',encoding="ISO-8859-1") as file:
                templates=file.readlines()
            with open(variable_path,'r',encoding="ISO-8859-1") as file:
                variables_dict=file.readlines()
            with open(template_id_path,'rb') as bfile:
                binary=bfile.read()
                index_list = elastic_decoder_bytes(binary)
                a = set(index_list )
            with open(variable_id_path,'rb') as bfile:
                binary=bfile.read()
                v_index_list = elastic_decoder_bytes(binary)

            for id in index_list:
                template_list.append(templates[id-1])
            replacements=[]
            for id in v_index_list:
                replacements.append(variables_dict[id-1])
            replacement_iter = iter(replacements)
            no_num_logs=self.replace_placeholders('<*>',template_list,replacement_iter)

            alpha = 'abcdefghijklmnopqrstuvwxyz'
            num_pattern = rf'_([a-zA-Z]+)_\.bin'
            for file in matching_files:

                    # 使用正则表达式搜索
                    match = re.search(num_pattern, file)
                    t_id=match.group(1)

                    if t_id == 'I':
                        ph = '<I>'
                        with open(file, 'rb') as bfile:
                            binary = bfile.read()
                            id_list = elastic_decoder_bytes(binary)
                        replacements = id_list
                        replacement_iter = iter(replacements)
                        no_num_logs = self.replace_placeholders( ph, no_num_logs, replacement_iter)
                    if t_id == 'a' or t_id == 'b':
                        ph='<'+str(t_id)+'>'
                        with open(file, 'rb') as bfile:
                            binary = bfile.read()
                            id_list = elastic_decoder_bytes(binary)
                        replacements = id_list
                        replacement_iter = iter(replacements)

                        no_num_logs = self.replace_placeholders( ph, no_num_logs, replacement_iter)


                    else:
                        ph='<'+str(t_id)+'>'
                        with open(file, 'rb') as bfile:
                            binary = bfile.read()
                            id_list = elastic_decoder_bytes(binary)
                        replacements=delta_transform_inverse(id_list)
                        replacement_iter = iter(replacements)
                        print(ph)
                        no_num_logs=self.replace_placeholders(ph,no_num_logs,replacement_iter)


            with open(out_put_path,'w',encoding="ISO-8859-1") as output_file:
                for log in no_num_logs:
                    output_file.write(log)
            chunkID += 1

    def replace_placeholders(self,placeholder,str_list, replacement_iter):

        replaced_list = []
        for s in str_list:
            new_s = s
            while placeholder in new_s:
                try:
                    replacement = next(replacement_iter)
                    new_s = new_s.replace(placeholder, str(replacement).strip(), 1)  # 只替换第一个匹配项
                except StopIteration:
                    raise ValueError("Compressed file is damaged: Not enough replacement variables to replace all "+placeholder)
            replaced_list.append(new_s)

        return replaced_list
    def extract_pos_number(self,filename):
        pattern = r'Apache(\d+)-(\d+)ids\.bin'
        match = re.search(pattern, filename)
        if match:
            return int(match.group(2))
        else:
            return float('inf')





    def find_files(self,directory, pattern):
        search_pattern = os.path.join(directory, pattern)
        file_list = glob.glob(search_pattern)
        return file_list




    def header_decompress(self,mapping_path,id_path):
        with open(id_path, 'rb') as id_file:
            ids_b=id_file.read()
            ids = elastic_decoder_bytes(ids_b)
        with open(mapping_path, 'r', encoding="ISO-8859-1") as mapping_file:
            mapping=mapping_file.readlines()

        header=[]
        for id in ids:
            id=id-1
            header.append(mapping[id])
        return header

    def content_decompress(self, mapping_path, id_path):
        with open(id_path, 'rb') as id_file:
            ids_b = id_file.read()
            ids = elastic_decoder_bytes(ids_b)
        with open(mapping_path, 'r', encoding="ISO-8859-1") as mapping_file:
            mapping = mapping_file.readlines()

        content = []
        for id in ids:
            id=id-1
            content.append(mapping[id])

        return content

    def number_padding(self,header,content,num_mapping,num_ids):
        lenth=len(header)
        header.extend(content)
        logs=header
        with open(num_ids, 'rb') as id_file:
            ids_b = id_file.read()
            ids = elastic_decoder_bytes(ids_b)
        with open(num_mapping, 'r', encoding="ISO-8859-1") as mapping_file:
            mapping = mapping_file.readlines()

        nums = []
        for id in ids:
            id=id-1
            nums.append(mapping[id])

        replacements = iter(nums)


        decompressed_logs = [
            re.sub(r"<\*>", lambda match: next(replacements).strip(), template)
            for template in logs
        ]

        try:
            count=0
            while True:
                a=next(replacements)
                print(str(a))
                count+=1
        except Exception as e:
            print('count =='+str(count))
        header=decompressed_logs[:lenth]
        content=decompressed_logs[lenth:]
        decompressed_logs=[str1.strip() +' '+ str2 for str1, str2 in zip(header, content)]
        return decompressed_logs





def delta_transform(num_list):
        initial=int(num_list[0])
        new_list=[initial]
        last=initial
        for item in num_list[1:]:
            delta=int(item)-int(last)
            new_list.append(delta)
            last=item
        return new_list

def delta_transform_inverse(num_list):
        initial=num_list[0]
        new_list=[initial]
        last=initial
        for item in num_list[1:]:
            inverse=int(item)+int(last)
            new_list.append(inverse)
            last=inverse
        return new_list

def get_file_size(file_path):
    if os.path.exists(file_path):
        file_size = os.path.getsize(file_path)
        return file_size
    else:
        return 0


def get_folder_size(folder_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            total_size += os.path.getsize(file_path)

    return total_size



def ppmd_compress_file(input_filename, compressed_filename, level=5, mem_size=16 << 20):
    with open(input_filename, 'rb') as input_file:
        data_to_compress = input_file.read()

    encoder = pyppmd.Ppmd8Encoder(level, mem_size)
    compressed_data = encoder.encode(data_to_compress)

    with open(compressed_filename, 'wb') as compressed_file:
        compressed_file.write(compressed_data)

def ppmd_decompress_file(compressed_filename, decompressed_filename, level=5, mem_size=16 << 20):
    with open(compressed_filename, 'rb') as compressed_file:
        compressed_data = compressed_file.read()

    decoder = pyppmd.Ppmd8Decoder(level, mem_size)
    decompressed_data = decoder.decode(compressed_data)

    with open(decompressed_filename, 'wb') as decompressed_file:
        decompressed_file.write(decompressed_data)


def create_tar(directory_path, tar_archive_name):
    with tarfile.open(tar_archive_name, 'w') as tar:
        for root, _, files in os.walk(directory_path):
            for file in files:
                full_path = os.path.join(root, file)
                arcname = os.path.relpath(full_path, start=directory_path)
                tar.add(full_path, arcname=arcname)


def compress_tar_with_ppmd(tar_archive_name, ppmd_archive_name, max_order=6, mem_size=16 << 20):
    with open(tar_archive_name, 'rb') as f_in:
        data_to_compress = f_in.read()

    encoder = pyppmd.Ppmd8Encoder(max_order, mem_size)
    compressed_data = encoder.encode(data_to_compress)

    with open(ppmd_archive_name, 'wb') as f_out:
        f_out.write(compressed_data)


def General7z_compress_lzma(dir):

    file_list=[]

    for root, dirs, files in os.walk(dir):
        for file in files:
            filename = os.path.join(root, file)
            file_list.append(filename)
    tarall = tarfile.open(os.path.join("{}/temp.tar.{}".format(dir, 'xz')), \
                              "w:{}".format('xz'))
    for idx, filepath in enumerate(file_list, 1):
            tarall.add(filepath, arcname=os.path.basename(filepath))
    tarall.close()


def General7z_compress_bzip2(dir):

    file_list=[]

    for root, dirs, files in os.walk(dir):
        for file in files:
            filename = os.path.join(root, file)
            file_list.append(filename)
    tarall = tarfile.open(os.path.join("{}/temp.tar.{}".format(dir, 'bz2')), \
                              "w:{}".format('bz2'))
    for idx, filepath in enumerate(file_list, 1):
            tarall.add(filepath, arcname=os.path.basename(filepath))
    tarall.close()

def create_and_empty_directory(directory_path):
    try:
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
        for item in os.listdir(directory_path):
            item_path = os.path.join(directory_path, item)
            if os.path.isfile(item_path):
                os.unlink(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
        print(f"Directory clean succeed: {directory_path}")
    except Exception as e:
        print(f"Directory clean failed: {e}")

#
# def zigzag_encoder(num: int):
#     return (num << 1) ^ (num >> 31)
#
#
# def zigzag_decoder(num: int):
#     return (num >> 1) ^ -(num & 1)
#
#
# def elastic_encoder(num: int):
#     # TODO: there're some bugs in elastic encoder
#     buffer = b''
#     cur = zigzag_encoder(num)
#     for i in range(4):
#         if (cur & (~0x7f)) == 0:
#             buffer += cur.to_bytes(1, "little")
#             # ret = i + 1
#             break
#         else:
#             buffer += ((cur & 0x7f) | 0x80).to_bytes(1, 'little')
#             cur = cur >> 7
#     return buffer
#
# def elastic_decoder_bytes(binary_bytes):
#     num_list=[]
#     num_byte=bytes()
#     for byt in binary_bytes:
#         num = int(byt)
#         byt = bytes([byt])
#         if num <128:
#             num_byte += byt
#             decode_num = int(elastic_decoder(num_byte))
#             num_list.append(decode_num)
#             num_byte=bytes()
#         else:
#             num_byte += byt
#     return num_list
#
#
# def elastic_decoder(num):
#
#     ret = 0
#     offset = 0
#     i = 0
#
#     while i < 5:
#         cur = num[i]
#         if (cur & (0x80) != 0x80):
#             ret |= (cur << offset)
#             i += 1
#             break
#         else:
#             ret |= ((cur & 0x7f) << offset)
#         i += 1
#         offset += 7
#
#     decode_num = zigzag_decoder(ret)
#     return decode_num
#

def zigzag_encoder(num: int):
    return (num << 1) ^ (num >> 63)


def zigzag_decoder(num: int):
    return (num >> 1) ^ -(num & 1)

def elastic_encoder(num: int):
    buffer = b''
    cur = zigzag_encoder(num)
    while True:
        # 如果当前剩余的数字在7位内，则直接输出
        if cur < 0x80:
            buffer += cur.to_bytes(1, "little")
            break
        else:
            # 输出当前的7位，并设置最高位为1
            buffer += ((cur & 0x7f) | 0x80).to_bytes(1, 'little')
            cur >>= 7
    return buffer

def elastic_decoder_bytes(binary_bytes):
    num_list = []
    num_byte = bytes()
    for byt in binary_bytes:
        num_byte += bytes([byt])
        # 如果最高位为0，表示当前数字结束
        if byt < 128:
            decode_num = elastic_decoder(num_byte)
            num_list.append(decode_num)
            num_byte = bytes()
    return num_list

def elastic_decoder(num_bytes):
    ret = 0
    offset = 0
    for i in range(len(num_bytes)):
        cur = num_bytes[i]
        ret |= ((cur & 0x7f) << offset)
        if cur & 0x80 == 0:
            break
        offset += 7
    return zigzag_decoder(ret)


class ExampleClass:
    def replace_and_group(self, lst):
        patterns = defaultdict(list)
        replaced = []
        entity_id = 1  # 初始ID号

        # 生成带有ID的实体替换模式
        def generate_entity_pattern():
            nonlocal entity_id
            pattern = f"<E{entity_id}>"
            entity_id += 1
            return pattern

        def replace_with_pattern(match):
            num = match.group()
            before = match.string[:match.start()]
            after = match.string[match.end():]
            if len(num) >= 4:
                pattern = f"<{len(num)}N{num[0]}>"
                patterns[pattern].append(num)
                return pattern
            elif before and after and before[-1].isalpha() and after[0].isalpha():
                entity_pattern = generate_entity_pattern()
                patterns[entity_pattern].append(num)
                return entity_pattern
            else:
                patterns['<*>'].append(num)
                return '<*>'
        for item in lst:
            replaced_item = re.sub(r'\d+', replace_with_pattern, item)
            replaced.append(replaced_item)

        return replaced, dict(patterns)