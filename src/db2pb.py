# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
import os, sys, io, re
import subprocess
import six, pprint, json
from itertools import groupby
from operator import itemgetter
import mariadb
import inspect
import shutil
import operator
import numpy as np
from PyInquirer import style_from_dict, Token, Separator
from PyInquirer import prompt, ValidationError, Validator
import pymongo, yaml
# from pymongo_schema.compare import compare_schemas_bases
# from pymongo_schema.export import transform_data_to_file
from pymongo_schema.extract import extract_pymongo_client_schema
# from pymongo_schema.filter import filter_mongo_schema_namespaces
# from pymongo_schema.tosql import mongo_schema_to_mapping

from termcolor import colored
from pyfiglet import figlet_format
from search import Searcher
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

def string_locals(template, **kwargs):
    if not kwargs:
        frame = inspect.currentframe()
        try:
            kwargs = frame.f_back.f_locals
        finally:
            del frame
        if not kwargs:
            kwargs = globals()
    return template.format(**kwargs)

def iterable(cls):
    def iterfn(self):
        iters = dict((x,y) for x,y in cls.__dict__.items() if x[:2] != '__')
        iters.update(self.__dict__)

        for x,y in iters.items():
            yield x,y

    cls.__iter__ = iterfn
    return cls

@iterable
class dbtype2pbtype(object):
    def __init__(self):
        self.string = "string"
        self.object =  "fixed64"
        self.array =  "bytes"
        self.binData =  "bytes"
        self.general_scalar = "google.protobuf.Any"
        self.undefined =  "google.protobuf.Any"
        self.objectId =  "google.protobuf.Any"
        self.oid =  "google.protobuf.Any"                               
        self.bool =  "bool"
        self.null =  "bytes"
        self.regex = "string"
        self.dbPointer =  "google.protobuf.Any"
        self.javascript =  "google.protobuf.Any"
        self.symbol =  "google.protobuf.Any"
        self.javascriptWithScope =  "google.protobuf.Any"
        self.long = "int64"
        self.minKey =  "sint32" 
        self.maxKey =  "sint32" 
        self.bigint =  "int64"
        self.int =  "int32"    
        self.integer =  "int32"
        self.numeric =  "int32"
        self.tinyint =  "sint32" 
        self.smallint =  "sint32"   
        self.year = "int32" 
        self.text =  "string"
        self.tinytext =  "string"    
        self.boolean =  "bool"
        self.Boolean =  "bool"        
        self.bit =  "bool"
        self.float =  "float"
        self.real =  "double"
        self.double =  "double"
        self.decimal =  "double"
        self.char =  "string"
        self.varchar =  "string"
        self.longvarchar =  "string"
        self.date =  "google.protobuf.Timestamp"
        self.datetime =  "google.protobuf.Timestamp"
        self.timestamp =  "google.protobuf.Timestamp"
        self.binary =  "bytes"
        self.varbinary =  "bytes"
        self.longvarbinary =  "bytes"
        self.blob =  "fixed64"
        self.clob =  "fixed64"
        self.enum = "Enum"
        self.set =  "string"

class Sql:
    def generate_proto(self):
        pass
class Nosql:
    def generate_proto(self):
        pass
    
def get_user_input():
    questions = [
        {
            "type": "checkbox",
            "message": "SQL or NoSQL: ",
            "name": "sqlnosql",
            "choices": [
                Separator("  "),
                {"name": "SQL" , "checked": False},
                {"name": "NoSQL", "checked": True},
            ],
        },
        {
            "type": "input",
            "name": "dbms",
            "message": "What's your DBMS?",
            "default": "mongodb",
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
            "default": "Squaddb",
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
            "default": "27017",
            "validate": NumberValidator,
            "filter": lambda val: int(val),
        },
        {
            "type": "input",
            "name": "proto_folder",
            "message": "Enter your working folder: ",
            "default": "proto",
        },
        {
            "type": "input",
            "name": "router_path",
            "message": "Enter your router path: ",
            "default": "C:\\_dev\\shiftschedulerback\\app\\routes",
        },                
        {
            "type": "input",
            "name": "api",
            "message": "Enter your rest api : ",
            "default": "/v1/",
        }
    ]
    # answers = prompt(questions, style=custom_style_2)
    answers = prompt(questions, style=style)
    #
    return answers

def generate_nosql_protos(collection, db_info):
    try:
        # folder = "proto"
        db2pb = dict(dbtype2pbtype())
                               
        for values in list(collection.values()): 
            for k, items in values.items():   
            # if k in routes:             
                log_string = f">>>>>> >>>>> Generating proto file {k.capitalize()} in folder {db_info['proto_folder']}"
                log(log_string, "yellow")                
                with open(k.capitalize() + '.proto', 'a') as the_file:
                    new_item_line = write_proto_head(the_file, db_info, k)          
                    index = 1
                    for item_k, item_val in items.items():
                        if type(item_val) == dict:
                            for item_kk, item_val_val in item_val.items():
                                if item_kk == '_id' or item_kk == 'createdAt' or  item_kk == 'updatedAt':
                                    continue
                                pbtype = db2pb.get(item_val_val['type'].lower())                                
                                write_line = "  {} {} = {}; ".format(pbtype, item_kk.ljust(len(item_kk) + 1), index) 
                                new_item_line.append(write_line)                                
                                the_file.write(write_line + '\n')                                        
                                index += 1
                    write_proto_bottom(the_file, k, new_item_line)

                    log_string = f">>>>>> >>>>> DONE Generating proto file {k.capitalize()} in folder {db_info['proto_folder']}"                                                         
                    log(log_string, "green")                 

    except Exception as err:
        err_string = f">>>>>> >>>>> Error Generating proto file {k.capitalize()} in folder {db_info['proto_folder']} : {err}"                                                                                   
        log(err_string, "red")  
        log(type(err), "red")                
        # print(type(err))    # the exception instance
        # print(err)          # __str__ allows args to be printed directly,
 
def generate_sql_protos(items, db_info):
    try:
        group_table = groupby(items, itemgetter(0))
        for kg, g in group_table:
            if kg == 'view' :
                continue
            table_tuple = [g[1:] for g in g]
        db2pb = dict(dbtype2pbtype())
        group_table = groupby(table_tuple, itemgetter(0))
        for k, g in group_table:
            required_list = []          
            my_option = "option (grpc.gateway.protoc_gen_swagger.options.openapiv2_schema) = { "
            my_json_schema = "\n json_schema : { \n"
            my_title = f"\t title : {k.capitalize()} \n"
            my_description=  "\t description : Does something neat \n"
            # my_required = f"\t required: string_locals(required_list, **locals()) \n }} \n }};" 
      
            with open(k + '.proto', 'a') as the_file:
                new_item_line = write_proto_head(the_file, db_info, k)
                field_list = []
                index = 1
                for tup in list(g):
                    pbtype = db2pb.get(tup[2])
                    required_list.append(tup[1])
                    if tup[2] != 'enum':
                        write_sub_line = f" {pbtype} {tup[1].ljust(len(tup[1]) + 1)} = {index};"
                        new_item_line.append(write_sub_line)
                        field_list.append(write_sub_line) 
                    else :
                        write_sub_line = " {} {} {} \t\t FALSE = 0; \n\
                                           TRUE = 1; \n {} {} {} = {};" \
                            .format(pbtype, tup[1].capitalize(), '{\n', index,'}\n', tup[1].capitalize(),tup[1], index)
                        new_item_line.append(write_sub_line)
                        field_list.append(write_sub_line) 
                    index += 1                    
                for field in field_list:
                    the_file.write(''.join(field) )                                                                                                
                    the_file.write('\n')  
                                                                                                                  
                option = f"{my_option} {my_json_schema} {my_title} {my_description} \trequired: {required_list} \n }} \n }};"                                                     
                the_file.write(option + '\n')                      

                write_proto_bottom(the_file, k, new_item_line)                            
        log("Generating proto files done", "green")   
    except Exception as err:
        print(type(err))    # the exception instance
        print(err)          # __str__ allows args to be printed directly,

def write_proto_head(the_file, db_info, k):
    write_line = "syntax = {}".format('"proto3";')            
    the_file.write(write_line + '\n')   

    write_line = 'import "google/protobuf/timestamp.proto";'         
    the_file.write(write_line + '\n')
    write_line = 'import "google/protobuf/any.proto";'
    the_file.write(write_line + '\n')    
    write_line = "import 'google/api/http.proto';"         
    the_file.write(write_line + '\n')
    write_line = "import 'google/api/httpbody.proto';"         
    the_file.write(write_line + '\n') 
    db_info['googleapis_path'] = db_info['googleapis_path'].replace('\\', '/')    
    # "{db_info["googleapis_path"]}/
    #write_line = f'import "protoc-gen-openapiv2/options/annotations.proto";'
    write_line = "import 'google/api/annotations.proto';"              
    the_file.write(write_line + '\n')                 
    write_line = "package {}.service.v1;".format(db_info["database"].capitalize())            
    the_file.write(write_line + '\n')
    the_file.write('\n')  
    option_go_package = f'option go_package = "github.com/yourorg/yourprotos/gen/go/{db_info["database"]}/service/v1";'        
    the_file.write(option_go_package + '\n')
    the_file.write('\n')  
    service_name = k.capitalize() + "Service"            
    write_line = "service {} {}".format(service_name, '{') 
    the_file.write(write_line + '\n')
    routes = db_info['routes']
    # if k.endswith('s'):
    #     k = k.removesuffix('s')  
    if 'get' in routes[k]:
        svc_read_option = "      option (google.api.http) = {{ get: '{}{}' \n}};".format(db_info['api'], k)   
        write_line = "  rpc {}({}Id) returns ({}) {{ \n \t {} }}".format(routes[k]['get'], k.capitalize(), k.capitalize(), svc_read_option)          
        the_file.write(write_line + '\n')
    if 'getAll' in routes[k]:        
        svc_list_option = "      option (google.api.http) = {{ get: '{}{}' \n}};\n".format(db_info['api'], k)
        write_line = "  rpc {}(Empty) returns ({}List) {{ \n \t {} }}".format(routes[k]['getAll'], k.capitalize(), svc_list_option)
        the_file.write(write_line + '\n')
    if 'post' in routes[k]:    
        svc_create_option = "      option (google.api.http) = {{  \n \t\t\t\t\t\t post: '{}{}' \n \t\t\t\t\t\t body: '*' \n}};\n".format(db_info['api'], k)         
        write_line = "  rpc {}(new{}) returns ({}) {{ \n \t {} }}".format(routes[k]['post'] , k.capitalize(), 'result', svc_create_option)          
        the_file.write(write_line + '\n')    
    if 'put' in routes[k] or 'patch' in routes[k]:
            svc_update_option = "      option (google.api.http) = {{ \n \t\t\t\t\t\tput: '{}{}' \n \t\t\t\t\t\t body: '*' \n}};\n".format(db_info['api'], k)        
            write_line = "  rpc {}({}) returns ({})  {{ \n \t {} }}".format(routes[k]['patch'] , k.capitalize(), 'result', svc_update_option)
            the_file.write(write_line + '\n')
    if 'delete' in routes[k]:        
        svc_delete_option = "      option (google.api.http) = {{ \n \t\t\t\t\t\tdelete: '{}{}' \n\t\t\t\t\t\t body: '*' \n}};\n".format(db_info['api'], k)        
        write_line = "  rpc {}({}) returns ({})  {{ \n \t {} }}".format(routes[k]['delete'] , k.capitalize(), 'result', svc_delete_option)
        the_file.write(write_line + '\n')
    write_line = "{}".format('}')  
    the_file.write(write_line + '\n')
    the_file.write('\n')
    new_item_line = []
    new_item_line.append("message new{} {}".format(k.capitalize(), '{') )                           
    write_line = "message {} {}".format(k.capitalize(), '{')
  
    the_file.write(write_line + '\n')          
    return new_item_line

def write_proto_bottom(the_file, k, new_item_line):
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

def get_nosql_schema(db_info):
    try:
        pymongo_client = pymongo.MongoClient(db_info["host"], 27017, maxPoolSize=50)
        db = pymongo_client[db_info["database"]]  
        collections = db.list_collection_names()
        for collection in enumerate(collections): 
            if collection[1] in db_info['routes']   :
                # if not operator.contains(collection[1], filename):
                #     continue                                                             
                coll_schema = extract_pymongo_client_schema(pymongo_client,
                                                                database_names=db_info["database"],
                                                                collection_names=collection)
                generate_nosql_protos(coll_schema, db_info )
    except Exception as err:
        err_string = f">>>>>> >>>>> Error Generating proto file {k.capitalize()} in folder {db_info['proto_folder']} : {err}"                                                                                   
        log(err_string, "red")  
        log(type(err), "red")           # __str__ allows args to be printed directly,   
    
def get_sql_schema(db_info):
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
    log("fetching db schema ", "green")
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
    log("db schema done", "green")
 
    generate_sql_protos(items, db_info)
    
    conn.commit()
    # Close Connection
    conn.close()

def compile_protos(googleapis_path):

    # protoc -I. -I"%GOPATH%/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis" 
    # --swagger_out=logtostderr=true:. webservice.proto  
    cwd = os.getcwd()
    print(cwd)       
    fs = os.listdir()
    for f in fs:
        if f.find(".proto")>-1:
            GOPATH = os.environ.get("gopath")            
            log_string = f"===== Compiling proto file {f} ====== "                                                                                   
            log(log_string, "blue")  
            command = (
                f"protoc --proto_path={cwd} "
                f"-I${GOPATH}/pkg/mod/google.golang.org/genproto@v0.0.0-20200513103714-09dca8ec2884/googleapis " 
                f"-I${GOPATH}/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.16.0/third_party/googleapis "	  
                f"-I${GOPATH}/pkg/mod/github.com/golang/protobuf@v1.5.2/ptypes "
                f"-I. "
                f"--plugin=protoc-gen-grpc-gateway=${GOPATH}/bin/protoc-gen-grpc-gateway --allow_delete_body=true "
                f"--cpp_out=plugins=grpc:. "
                f"--grpc-gateway_out=logtostderr=true:allow_delete_body=true:. "
                f"{f}"
            )
            # command=(
            #     f'protoc --proto_path={cwd} '
            #     f'--proto_path={googleapis_path} '
            #     f'--swagger_out=logtostderr=true:. '
            #     f'--cpp_out=./ --grpc_cpp_out=./server ' 
            #     f'{f}'               
            # )
            
            print(command)
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
        err_string = f"======= Error: failed to execute command: {cmd}"                                                                                   
        log(err_string, "red") 
        err_string = f"======= Return error: {error}"                                                                                   
        log(err_string, "yellow")         
        # print( "Error: failed to execute command:", cmd)
        # print(error)
        return error
    return result

# @click.command()
def main():
    """
    Simple CLI
    """
    log(">>>>> Welcome to DB2PB CLI", color="blue", figlet=False)
    # log("CLI : >", color="blue", figlet=True)
   
    with open("./src/config.yaml", "r") as yamlfile:
        data = yaml.load(yamlfile, Loader=yaml.FullLoader)

    # db_info = get_user_input()
    db_info = data[0]['db_info'] 
    if not os.path.exists(db_info['proto_folder']):
        os.mkdir(db_info['proto_folder'])
    os.chdir(db_info['proto_folder'])

    cwd = os.getcwd()
    # print(cwd)     
    for filename in os.listdir(cwd):
        file_path = os.path.join(cwd, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))       
    # print(''.join(db_info["sqlnosql"]))  
    verbs = ".get|.post|.patch|.delete"    
    Search = Searcher(db_info['router_path'], verbs)
    Search.find()
    db_info['routes'] = Search.getResults()
    if ''.join(db_info["sqlnosql"]) == 'SQL' :
        get_sql_schema(db_info)
    else:  
        get_nosql_schema(db_info)  
    compile_protos(db_info['googleapis_path'])

if __name__ == "__main__":
    main()
