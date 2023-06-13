#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on March 22 - 2022

@author: alexander Robles
"""
from config import *
import requests
import pandas as pd
import json

step_cap= {
    'ciencuadras': [raw_arriendo, raw_venta,raw_Proyectos_Nuevos],
    #'fincaraiz': [raw_fc_renta]
}


def Ciencuadras_Request(url, raw):
    r = requests.post(url, data=raw)
    json_body = json.loads(raw)
    #name file
    name = json_body['pathurl']
    #_____
    data = json.loads(r.content)
    
    paginas = data['data']['totalPages']
    df = pd.DataFrame(json.loads(r.content)['data']['result'])
    for p in range(2,paginas):
        json_body['numberPaginator']=p
        raw_mod = json.dumps(json_body)
        r = requests.post(url, data=raw_mod)
        df_new = pd.DataFrame(json.loads(r.content)['data']['result'])
        df = pd.concat([df,df_new])
        print(p)
    return df, name


if __name__ == '__main__':
    for k,v in step_cap.items():
        if k == 'ciencuadras':
            for transaction in v:
                to_export, nombre = Ciencuadras_Request(url_ciencuadras, transaction)
                # CUDADO ACA esta linea estaba en local
                to_export.to_csv("./descarga/Abril_2023_%s.csv" % nombre, encoding='utf8')
       
    






