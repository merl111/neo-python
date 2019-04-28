import rocksdb


class StaticPrefix(rocksdb.interfaces.SliceTransform):                                                                                                                                                                                                                      
    def name(self):                                                             
        return b'static'                                                        

    def transform(self, src):                                                   
        return (0, 2)                                                           

    def in_domain(self, src):                                                   
        return len(src) >= 2                                                    

    def in_range(self, dst):                                                    
        return len(dst) == 2 
