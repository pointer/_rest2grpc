import os
import mmap
import re
import contextlib

class Searcher:
    def __init__(self, path, query):
        self.path   = path

        if self.path[-1] != '/':
            self.path += '/'

        self.path = self.path.replace('/', '\\')

        self.query  = re.compile(br'\b(%s)\b'%query.encode(), flags=re.IGNORECASE) 
        self.searched = {}

    def find(self):
        for root, dirs, files in os.walk( self.path ):
            for file in files:           
                if re.match(r'.*?\.js$', file) is not None:
                    if root[-1] != '\\':
                        root += '\\'           
                    with open(root + file, 'rb') as f:
                        with contextlib.closing(mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)) as m:
                            dict_item= {}
                            base=os.path.basename(file)
                            (file, ext) = os.path.splitext(base)
                            # if file.endswith('s'):
                            #     file = file.removesuffix('s')           
                            get = ''
                            for match in self.query.findall(m):
                                new_verb = match.decode('utf-8').removeprefix(".")
                                if new_verb == 'get' and get == 'get':
                                    # file = file + 's'
                                    new_verb = new_verb + 'All'
                                    get = ''
                                # elif file.endswith('s'):
                                #     file = file.removesuffix('s')                                                                          
                                dict_item.update({new_verb : new_verb + file.capitalize()})
                                get = new_verb
                            self.searched[file] = dict_item

    def getResults(self):
        return self.searched