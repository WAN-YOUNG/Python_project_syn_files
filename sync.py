import sys
import yaml
import os
import win32file
import win32con
import paramiko
import argparse
import subprocess
import time
import datetime
import hashlib

g_dbg_log = 0

def time_stamp():
  return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def debug_output(str):
  if g_dbg_log == 1:
    print('%s' % str)

def blue(str):
  print('===blue===[{}]: {}'.format(time_stamp(), str))

def red(str):
  print('===red===[{}]: {}'.format(time_stamp(), str))

def is_contain_chinese(str):
  for ch in str:
    if u'\u4e00' <= ch <= u'\u9fff':
      return True
  return False

def is_contain_space(str):
  if len(str.split(' ', 1)) == 2:
    return True
  else:
    return False

def hash_file(file):
  hash_obj = hashlib.md5()
  with open(file,'rb') as f:
      for line in f:
          hash_obj.update(line)
  return hash_obj.hexdigest()

def is_contain_chinese_space(str):
  return is_contain_chinese(str) | is_contain_space(str)

class sync:
  def __init__(self, def_config_file_path):   
    self.def_config_file_path = def_config_file_path  # 默认配置文件地址

  def load_yaml(self, yaml_path):
    hf = open(yaml_path,'r')
    yaml_content = hf.read()
    self.yaml_parse_content = yaml.load(yaml_content)
    blue("load config yaml from {}".format(yaml_path))

  def get_yaml_para(self):
    self.ip = self.yaml_parse_content['ip']
    self.port = self.yaml_parse_content['port']
    self.user = self.yaml_parse_content['user']
    self.passwd = self.yaml_parse_content['passwd']
    self.local_root_path = self.yaml_parse_content['local_root_path']
    self.remote_root_path = self.yaml_parse_content['remote_root_path']
    self.files = self.yaml_parse_content['files']
    self.ignore = self.yaml_parse_content['ignore']
    self.key_file = self.yaml_parse_content['key_file']
    self.is_use_key = False
    self.retry = 0  # 文件push/fetch失败重试次数
    blue("get para from config yaml success")
  
  def ssh_exec_cmd(self, cmd):
    #创建ssh客户端
    client = paramiko.SSHClient()
    #第一次ssh远程时会提示输入yes或者no
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    if self.is_use_key == False:
      #密码方式远程连接
      client.connect(self.ip, self.port, self.user, self.passwd, timeout=20)
    else:
      #互信方式远程连接
      key_file = paramiko.RSAKey.from_private_key_file(self.key_file, password=self.passwd)
      client.connect(self.ip, self.port, username=self.user, pkey=key_file, timeout=20)

    #执行命令
    stdin, stdout, stderr = client.exec_command(cmd)
    #获取命令执行结果,返回的数据是一个list
    ssh_exec_result = stdout.readlines()
    debug_output("{} result is [{}]".format(cmd, ssh_exec_result))
    return ssh_exec_result

  def is_ignore(self, file):
    full_filename = r"{}\{}".format(self.local_root_path.replace("/","\\"), file.replace("/","\\"))
    for i in self.ignore:
      tem = r"{}\{}".format(self.local_root_path.replace("/","\\"), i.replace("/","\\"))
      if os.path.isfile(tem) == True:
        if tem == full_filename:
          return True
      else:  # 文件属于忽略目录
        if tem in full_filename:
          return True
    return False
  
  def constuct_prefix_push_cmd(self):
    push_cmd = "pscp -pw {} -P {}".format(self.passwd, self.port)
    if self.is_use_key == True:
      push_cmd = "{} -i {}".format(push_cmd, self.key_file.replace("/","\\"))
    return push_cmd

  def constuct_suffix_push_cmd(self, file):
    local_file = r"{}\{}".format(self.local_root_path.replace("/","\\"), file.replace("/","\\"))
    remote_file = "{}@{}:{}/{}".format(self.user, self.ip, self.remote_root_path.replace("\\","/"), file.replace("\\","/"))  # linux路径分割符用/
    if self.is_fetch == False:
      push_cmd = "{} {}".format(local_file, remote_file)
    else:
      push_cmd = "{} {}".format(remote_file, local_file)
    return push_cmd

  def create_dir(self, file):
    if self.is_fetch == False:
      remote_path = os.path.split("{}/{}".format(self.remote_root_path.replace("\\","/"), file.replace("\\","/")))[0]
      cmd = "mkdir -p {}".format(remote_path)
      self.ssh_exec_cmd(cmd)
      debug_output("{}@{}:{} exec [{}]".format(self.user, self.ip, self.port, cmd))
    else:
      local_path = os.path.split(r"{}\{}".format(self.local_root_path.replace("/","\\"), file.replace("/","\\")))[0]
      if not os.path.exists(local_path):
        os.makedirs(local_path)
        blue("local path:{} is not exist and mkdir".format(local_path))

  def is_md5_remote_consistency_with_local(self, file):
    try:
      cmd = "md5sum {}/{}".format(self.remote_root_path.replace("\\","/"), file.replace("\\","/"))
      remote_md5 = self.ssh_exec_cmd(cmd)[0].split()[0]
      debug_output("remote md5 = {}".format(remote_md5))

      local_md5 = hash_file(r"{}\{}".format(self.local_root_path.replace("/","\\"), file.replace("/","\\")))
      debug_output("locale md5 = {}".format(local_md5))

      blue("file: {} md5[remote:{}, local:{}], flag:{}[True:same, False:not same]".format(os.path.split(file)[1], remote_md5, local_md5, remote_md5 == local_md5))

      if remote_md5 == local_md5:
        return True
      return False
    except IndexError:
      red("local or remote file:{} is not exist".format(os.path.split(file)[1]))
      return False

  def push_file(self, file):
    blue("{} file={} {}, retry {} times".format('=' * 20, file, '=' * 20, self.retry))
    
    if self.is_ignore(file) == True:
      red(r"ignore file to sync : {}\{}".format(self.local_root_path.replace("/","\\"), file.replace("/","\\")))
      return

    try:
      # 判断MD5值是否一致，若一致则不需要传输
      if self.is_md5_remote_consistency_with_local(file) == True:
        red("no sync file：{}".format(os.path.split(file)[1]))
        return

      # 由于push/fetch之前可能远端/本地并没有对应的文件夹，这里直接创建文件夹
      self.create_dir(file)

      # 推送文件到远端or取文件到本地    
      cmd = r"{} {}".format(self.constuct_prefix_push_cmd(), self.constuct_suffix_push_cmd(file))
      if self.is_fetch == False:
        debug_output("push file cmd : {}".format(cmd))
      else:
        debug_output("fetch file cmd : {}".format(cmd))
      
      ret = os.system(cmd)
      if ret != 0:
        red(r"push file {}\{} to {}@{}:{}{}/{} failed!".format(self.local_root_path.replace("/","\\"), file.replace("/","\\"), 
              self.user, self.ip, self.port, self.remote_root_path.replace("\\","/"), file.replace("\\","/")))
        return
      blue(r"push file {}\{} to {}@{}:{}{}/{} success!".format(self.local_root_path.replace("/","\\"), file.replace("/","\\"), 
            self.user, self.ip, self.port, self.remote_root_path.replace("\\","/"), file.replace("\\","/")))
      
      self.retry = 1
    except PermissionError:
      red("file {} is busy".format(file.replace("/","\\")))
      self.retry += 1
      if self.retry >= 6:  # 最多操作5次
        red("file {} retry max times(5)".format(file.replace("/","\\")))
        return
      time.sleep(1)
      self.push_file(file)

  def sync_cust_files(self):
    red("==============================coustom files to sync================================")

    if len(self.files) == 0:
      blue("no files to synization")
      return

    # 指定files列表模式同步
    for file in self.files:
      self.push_file(file)

  def sync_auto(self):
    red("==============================auto to synchronize files================================")

    ACTIONS = {
      1 : "Created",
      2 : "Deleted",
      3 : "Updated",
      4 : "Renamed from something",
      5 : "Renamed to something"
    }

    FILE_LIST_DIRECTORY = win32con.GENERIC_READ | win32con.GENERIC_WRITE
    path_to_watch = self.local_root_path
    hDir = win32file.CreateFile (
      path_to_watch,
      FILE_LIST_DIRECTORY,
      win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE,
      None,
      win32con.OPEN_EXISTING,
      win32con.FILE_FLAG_BACKUP_SEMANTICS,
      None
    )

    while 1:
      results = win32file.ReadDirectoryChangesW (
                                              hDir,  #handle: Handle to the directory to be monitored. This directory must be opened with the FILE_LIST_DIRECTORY access right.
                                              1024,  #size: Size of the buffer to allocate for the results.
                                              True,  #bWatchSubtree: Specifies whether the ReadDirectoryChangesW function will monitor the directory or the directory tree. 
                                              win32con.FILE_NOTIFY_CHANGE_FILE_NAME |
                                              win32con.FILE_NOTIFY_CHANGE_DIR_NAME |
                                              win32con.FILE_NOTIFY_CHANGE_ATTRIBUTES |
                                              win32con.FILE_NOTIFY_CHANGE_SIZE |
                                              win32con.FILE_NOTIFY_CHANGE_LAST_WRITE |
                                              win32con.FILE_NOTIFY_CHANGE_SECURITY,
                                              None,
                                              None)
      
      last_name = ""  # 预分配重命名文件/目录更改名字之前的变量
      for action, file in results:
          full_filename = os.path.join (path_to_watch, file)
          print(full_filename, ACTIONS.get(action, "Unknown"))
   
          if is_contain_chinese_space(full_filename) == False:
            if action == 1:  # create
              if os.path.isfile(full_filename) == True:
                cmd = "touch {}/{}".format(self.remote_root_path.replace("\\","/"), file.replace("\\","/"))
                self.ssh_exec_cmd(cmd)
                blue("{}@{}:{} exec [{}]".format(self.user, self.ip, self.port, cmd))
              else:
                cmd = "mkdir -p {}/{}".format(self.remote_root_path.replace("\\","/"), file.replace("\\","/"))
                self.ssh_exec_cmd(cmd)
                blue("{}@{}:{} exec [{}]".format(self.user, self.ip, self.port, cmd))
            elif action == 2:  # delete
              cmd = "rm -r {}/{}".format(self.remote_root_path.replace("\\","/"), file.replace("\\","/"))
              self.ssh_exec_cmd(cmd)
              blue("{}@{}:{} exec [{}]".format(self.user, self.ip, self.port, cmd))
            elif action == 3:  # update
              if os.path.isfile(full_filename) == True:  # 文件才需要push到远端              
                self.push_file(file)
            elif action == 4:  # rename from
              last_name = "{}/{}".format(self.remote_root_path.replace("\\","/"), file.replace("\\","/"))
              print("last = ",last_name)
            elif action == 5:  # rename to              
              if len(last_name) != 0: # 正常的重命名操作
                now_name = "{}/{}".format(self.remote_root_path.replace("\\","/"), file.replace("\\","/"))
                cmd = "mv {} {}".format(last_name, now_name)
                self.ssh_exec_cmd(cmd)
                blue("{}@{}:{} exec [{}]".format(self.user, self.ip,self.port, cmd))
              else:  # 若之前的目录/文件中有空格，last_name会为空，被is_contain_space拦截，这里再次将本地的目录/文件在远端操作一遍
                if os.path.isfile(full_filename) == True:  # 文件直接再push到远端一次
                  self.push_file(file)
                else:  # 目录在远端重新再建立一个
                  cmd = "mkdir -p {}/{}".format(self.remote_root_path.replace("\\","/"), file.replace("\\","/"))
                  self.ssh_exec_cmd(cmd)
                  blue("{}@{}:{} exec [{}]".format(self.user, self.ip, self.port, cmd))
              last_name = ""
          else:
            red("we can't deal path with space and chinese({})".format(full_filename))  # linux不支持路径/文件名中带空格

  def parse_arg(self):
    parser = argparse.ArgumentParser(description = "sync files as your mind")
    parser.add_argument("-c", "-C", "--custom", action = 'store_true', help = "use the files field to specify the files to be synchronized")
    parser.add_argument("-a", "-A", "--auto", action = 'store_true', help = "automatic synchronization when saved")
    parser.add_argument("-f", "-F", "--fetch", action = 'store_true', help = "fetch file from remote server, only use in custome mode")
    parser.add_argument("-d", "-D", "--debug", action = 'store_true', help = "set debug flag")
    parser.add_argument("-s", "-S", "--set", nargs = 1, dest = "config_file", help = r"the path of config files, default path is C:\Users\setting.yaml")
    #parser.add_argument("-k", "-K", "--key", action = 'store_true', help = r"use the key file to connect remote server") 
    self.args = parser.parse_args()
 
  def run(self):
    if self.args.custom == False and self.args.auto == False:
      blue("you can input -h or --help to get help,such as : sync -h")
      return

    if self.args.auto == True and self.args.fetch == True:
      red("we can fetch file only use in custome mode")
      return

    if self.args.debug:
      global g_dbg_log
      g_dbg_log = 1

    #self.is_use_key = self.args.key
    self.is_fetch = self.args.fetch

    if self.args.config_file:
      self.load_yaml(self.args.config_file[0])
    else:
      self.load_yaml(self.def_config_file_path)
    self.get_yaml_para()

    if self.args.custom:
      self.sync_cust_files()
    elif self.args.auto:
      self.sync_auto()     

if __name__ == "__main__":
  sync_obj = sync(r"C:\Users\setting.yaml")
  sync_obj.parse_arg()
  sync_obj.run()