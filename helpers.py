import binascii as ba

def pp_tuple(triple):
    return tuple(i for i in t2hex(triple))

def pp_id2s(triple,stringstore):
    return tuple(stringstore.id2s(i) for i in triple)
        
def t2hex(triple):
    for key in triple:
        if key is None:
            yield "None"
        else:
            yield ba.hexlify(key)
    