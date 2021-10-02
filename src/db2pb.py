# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
import os, sys, io
import subprocess
import six
from itertools import groupby
from operator import itemgetter
import mariadb
import numpy as np
from PyInquirer import style_from_dict, Token, Separator
from PyInquirer import prompt, ValidationError, Validator
from termcolor import colored
from pyfiglet import figlet_format

try:
    import colorama

    colorama.init()
except ImportError:
    colorama = None

try:
    from termcolor import colored
except ImportError:
    colored = None

style = style_from_dict(
    {
        Token.QuestionMark: "#fac731 bold",
        Token.Answer: "#4688f1 bold",
        Token.Instruction: "",  # default
        Token.Separator: "#cc5454",
        Token.Selected: "#0abf5b",  # default
        Token.Pointer: "#673ab7 bold",
        Token.Question: "",
    }
)

def traverse(value, key=None):
    if isinstance(value, dict):
        for k, v in value.items():
            yield from traverse(v, k)
    else:
        yield key, value


def correct_encoding(dictionary):
    """Correct the encoding of python dictionaries so they can be encoded to mongodb
    inputs
    -------
    dictionary : dictionary instance to add as document
    output
    -------
    new : new dictionary with (hopefully) corrected encodings"""

    new = {}
    for key1, val1 in dictionary.items():
        # Nested dictionaries
        if isinstance(val1, dict):
            val1 = correct_encoding(val1)

        if isinstance(val1, np.bool_):
            val1 = bool(val1)

        if isinstance(val1, np.int64):
            val1 = int(val1)

        if isinstance(val1, np.float64):
            val1 = float(val1)

        new[key1] = val1

    return new


def log(string, color, font="slant", figlet=False):
    if colored:
        if not figlet:
            six.print_(colored(string, color))
        else:
            six.print_(colored(figlet_format(string, font=font), color))
    else:
        six.print_(string)


class FilePathValidator(Validator):
    def validate(self, value):
        if len(value.text):
            if os.path.isfile(value.text):
                return True
            else:
                raise ValidationError(message="File not found", cursor_position=len(value.text))
        else:
            raise ValidationError(
                message="You can't leave this blank", cursor_position=len(value.text)
            )


class NumberValidator(Validator):
    def validate(self, document):
        try:
            int(document.text)
        except ValueError:
            raise ValidationError(
                message="Please enter a number", cursor_position=len(document.text)
            )  # Move cursor to end


# class myThread (threading.Thread):
#     def __init__(self, threadID, name, counter, redisOpsObj):
#         threading.Thread.__init__(self)
#         self.threadID = threadID
#         self.name = name
#         self.counter = counter
#         self.redisOpsObj = redisOpsObj

#     def stop(self):
#         self.kill_received = True

#     def sample(self):
#         print "Hello"

#     def run(self):
#         time.sleep(0.1)
#         print "\n Starting " + self.name
#         self.sample()


def askInformation():
    questions = [
        {
            "type": "checkbox",
            "message": "SQL or NoSQL: ",
            "name": "sqlnosql",
            "choices": [
                Separator("  "),
                {"name": "SQL", "checked": True},
                {"name": "NoSQL"},
            ],
        },
        {
            "type": "input",
            "name": "dbms",
            "message": "What's your DBMS?",
            "default": "mariadb",
        },
        {
            "type": "input",
            "name": "username",
            "message": "Enter your username: ",
            "default": "root",
        },
        {
            "type": "password",
            "message": "Enter your password: ",
            "name": "password",
            "default": "password",
        },
        {
            "type": "input",
            "name": "database",
            "message": "Enter your db name: ",
            "default": "sakila",
        },
        {
            "type": "input",
            "name": "host",
            "message": "Enter your db host: ",
            "default": "localhost",
        },
        {
            "type": "input",
            "name": "port",
            "message": "Enter your db port: ",
            "default": "3306",
            "validate": NumberValidator,
            "filter": lambda val: int(val),
        },
        {
            "type": "input",
            "name": "proto",
            "message": "Enter your working folder: ",
            "default": "../proto",
        },
    ]
    # answers = prompt(questions, style=custom_style_2)
    answers = prompt(questions, style=style)

    return answers


def generate_files(items):
    try:
        group_table = groupby(items, itemgetter(0))
        for k, g in group_table:
            if k == 'view' :
                continue
            table_tuple = [g[1:] for g in g]

        folder = "proto"
        os.system("echo generating proto files")
        if not os.path.exists(folder):
            os.mkdir(folder)
        os.chdir(folder)
        import shutil
        cwd = os.getcwd()
        print(cwd)     
        for filename in os.listdir(cwd):
            file_path = os.path.join(cwd, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))
        group_table = groupby(table_tuple, itemgetter(0))
        for k, g in group_table:
            with open(k + '.proto', 'a') as the_file:
                write_line = "syntax = {}".format("'proto3';")            
                the_file.write(write_line + '\n')   

                write_line = "import 'google/protobuf/timestamp.proto';"         
                the_file.write(write_line + '\n')
                
                write_line = "package {};".format(k.capitalize())            
                the_file.write(write_line + '\n')
                the_file.write('\n')               
                service_name = k.capitalize() + "Service"            
                write_line = "service {} {}".format(service_name, '{') 
                the_file.write(write_line + '\n')
                write_line = "  rpc list{}s(Empty) returns ({}List) {}".format(k.capitalize(), k.capitalize(), '{}')
                the_file.write(write_line + '\n')
                write_line = "  rpc read{}({}Id) returns ({}) {}".format(k.capitalize(), k.capitalize() , k.capitalize(), '{}')          
                the_file.write(write_line + '\n')  
                write_line = "  rpc create{}(new{}) returns ({}) {}".format(k.capitalize() , k.capitalize(), 'result', '{}')          
                the_file.write(write_line + '\n')
                write_line = "  rpc update{}({}) returns ({}) {}".format(k.capitalize() , k.capitalize(), 'result', '{}')          
                the_file.write(write_line + '\n')                         
                write_line = "  rpc delete{}({}) returns ({}) {}".format(k.capitalize() , k.capitalize(), 'result', '{}')          
                the_file.write(write_line + '\n')
                write_line = "{}".format('}')  
                the_file.write(write_line + '\n')
                the_file.write('\n')
                new_item_line = []
                new_item_line.append("message new{} {}".format(k.capitalize(), '{') )                           
                write_line = "message {} {}".format(k.capitalize(), '{') 
                the_file.write(write_line + '\n')          
                index = 1
                field_type = '' 
                field = ''
                for tup in list(g):
                    field = tup[1]
                    if (
                        tup[2] == "int"
                        or tup[2] == "integer"
                        or tup[2] == "tinyint"
                        or tup[2] == "smallint"
                        or tup[2] == "mediumint"                        
                        or tup[2] == "numeric"
                        or tup[2] == "year"                        
                    ):
                        field_type = 'int32'
                        write_line = "  int32 {} = {}; ".format(tup[1].ljust(len(tup[1]) + 1), index) 
                        new_item_line.append(write_line)
                    elif tup[2] == "bigint" :
                        field_type = 'int64'                
                        write_line = "  int64 {} = {}; ".format(tup[1], index)       
                        new_item_line.append(write_line)                                     
                    elif (
                        tup[2] == "text"
                        or tup[2] == "tinytext"
                        or tup[2] == "char"
                        or tup[2] == "varchar"
                        or tup[2] == "longvarchar"
                        or tup[2] == "set"                              
                    ):
                        field_type = 'string'
                        write_line = "  string {} = {}; ".format(tup[1].ljust(len(tup[1]) + 1), index)
                        new_item_line.append(write_line)
                    elif tup[2] == "enum" or tup[2] == "set":
                        field_type = 'enum'                
                        write_line = "  enum {} {} \t\t TBD{} = 0; \n {} {} {} = {};" \
                            .format(tup[1].capitalize(), '{\n', index,'}\n', tup[1].capitalize(),tup[1], index)
                        new_item_line.append(write_line)                         
                    elif tup[2] == "bool" or tup[2] == "boolean":
                        field_type = 'bool'                
                        write_line = "  bool {} = {};".format(tup[1], index)
                        new_item_line.append(write_line)         
                    elif tup[2] == "real" or tup[2] == "double" or tup[2] == "decimal":
                        field_type = 'double'                
                        write_line = "  double {} = {}; ".format(tup[1], index)
                        new_item_line.append(write_line)                    
                    elif tup[2] == "float":
                        field_type = 'float'                
                        write_line = "  float {} = {}; ".format(tup[1], index)
                        new_item_line.append(write_line)                               
                    elif tup[2] == "date" or tup[2] == "time" or tup[2] == "timestamp" or tup[2] == "datetime":
                        field_type = 'google.protobuf.Timestamp'                
                        write_line = "  google.protobuf.Timestamp {} = {}; ".format(tup[1].ljust(len(tup[1]) + 1), index)
                        new_item_line.append(write_line)                             
                    elif (
                        tup[2] == "blob"
                        or tup[2] == "clob"
                    ):
                        field_type = 'bytes'                
                        write_line = "  bytes {} = {}; ".format(tup[1], index)
                        new_item_line.append(write_line)                    
                    elif (
                        tup[2] == "binary"
                        or tup[2] == "varbinary"
                        or tup[2] == "longvarbinary"  ):
                        field_type = 'fixed64'                
                        write_line = "  fixed64 {} = {}; ".format(tup[1], index)
                        new_item_line.append(write_line)                    
                    index += 1
                    the_file.write(write_line + '\n')
                write_line = "{}".format('}')             
                the_file.write(write_line + '\n')
                the_file.write('\n')             
                write_line = "message {} {}".format('Empty', '{}') 
                the_file.write(write_line + '\n')  
                the_file.write('\n')

                write_line = "message {}List {}".format(k.capitalize(), '{') 
                the_file.write(write_line + '\n')
                write_line = "  repeated {} {}s = 1;".format(k.capitalize() , k)
                the_file.write(write_line + '\n')              
                write_line = "{}".format('}')  
                the_file.write(write_line + '\n')
                the_file.write('\n')            
                                                                        
                write_line = "message {}Id {}".format(k.capitalize(), '{') 
                the_file.write(write_line + '\n')
                write_line = "  int32 id = 1;" 
                the_file.write(write_line + '\n')              
                write_line = "{}".format('}')
                the_file.write(write_line + '\n')             
                the_file.write('\n')   
                for item in new_item_line:
                    the_file.write("%s\n" % item)            
                write_line = "{}".format('}')
                the_file.write(write_line + '\n')
                write_line = "message result {}".format('{') 
                the_file.write(write_line + '\n')
                write_line = "  string status = 1;"
                the_file.write(write_line + '\n')              
                write_line = "{}".format('}')  
                the_file.write(write_line)                            
   
    except Exception as err:
        print(type(err))    # the exception instance
        print(err)          # __str__ allows args to be printed directly,

def get_data(db_info):
    try:
        conn = mariadb.connect(
            user=db_info["username"],
            password=db_info["password"],
            host=db_info["host"],
            port=db_info["port"],
            database=db_info["database"],
        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

    # Get Cursor
    cur = conn.cursor()
    cur.execute(
            "select CASE WHEN b.TABLE_NAME is not null then 'view' else 'table' end OBJECT_TYPE, \
                a.table_name, a.column_name, a.data_type, a.ordinal_position \
            from INFORMATION_SCHEMA.COLUMNS a \
            LEFT OUTER JOIN INFORMATION_SCHEMA.VIEWS b ON a.TABLE_CATALOG = b.TABLE_CATALOG \
                AND a.TABLE_SCHEMA = b.TABLE_SCHEMA AND a.TABLE_NAME = b.TABLE_NAME \
            WHERE a.table_schema=DATABASE();"
    )
    # SELECT table_name, column_name, data_type, ordinal_position FROM information_schema.COLUMNS WHERE table_schema=DATABASE();

    items = cur.fetchall()
    # for write_line in items:
    #     pprint(write_line)
    generate_files(items)
    
    conn.commit()
    # Close Connection
    conn.close()

def compile_protos():
    fs = os.listdir()
    for f in fs:
        if f.find(".proto")>-1:
            print(f)
            command='protoc '+f+' --cpp_out=.'
            print("This process detail: \n", execute(command))
            # os.system(s)

def execute(cmd):
    """
        Purpose  : To execute a command and return exit status
        Argument : cmd - command to execute
        Return   : exit_code
    """
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (result, error) = process.communicate()
    rc = process.wait()
    if rc != 0:
        print( "Error: failed to execute command:", cmd)
        print(error)
    return result

# @click.command()
def main():
    """
    Simple CLI
    """
    log("Welcome to DB2PB CLI", "green")
    log("CLI : >", color="blue", figlet=True)

    db_info = askInformation()
    get_data(db_info)
    compile_protos()

if __name__ == "__main__":
    main()
