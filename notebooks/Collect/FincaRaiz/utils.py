# Databricks notebook source
import os
import json
import geojson
import requests

from shapely.geometry import shape
from shapely import geometry, ops
from PIL import Image


# CAPTURA CIENCUADRAS
def item_FincaRaiz(response, list_extract, oferta):
    """Esta funcion permite organizar los valores devueltos por el json, organizarlo y almacenarlo en un dataframe"""
  # extrae las columnas son utiles para almacenar
    df_filter = {k:v for (k,v) in json.loads(response.content)['hits']['hits'][oferta]['_source']['listing'].items() if k in list_extract}
    peticion = pd.json_normalize(df_filter)


  #Borra columnas del dataframe
    drop_list = ['locations.view_map.id','locations.view_map.name','locations.view_map.slug',
              'locations.countries','locations.states','locations.cities', 
              'locations.neighbourhoods', 'locations.groups','client.logo.full_size',
              'client.logo.detail','client.logo.list','client.logo.thumb','seo.url',
               'seo.keyword', 'seo._rule', 'garages.id', 'garages.slug','rooms.id',
               'rooms.slug', 'stratum.id', 'baths.id', 'condition.id','age.id', 
               'client.client_id', 'client.logo', 'floor.id'
               ]
    for i in drop_list:
        try:
            peticion = peticion.drop([i], axis=1)
        except:
            pass

  # Los datos anidados son extraidos
    for a in peticion.columns:
        value = peticion[a].values[0]
        tipo = type(value)
        if tipo is list:
            if len(value)>0:
                nest_dic = value[0]
                if type(nest_dic) is dict:
                    for k,v in nest_dic.items():
                        if k in ['name','email','phone_number']:
                            peticion[a] = v
                        else:
                            pass
                else:
                    pass
        elif tipo is str:
            if a == 'address':
                v_address = json.loads(value)
                peticion[a]=v_address['address']
        else:
            pass
    return peticion

# AUTOCORRELACIONES
def fill_hexagons(geom_geojson, res, flag_swap = False, flag_return_df = False):
    """Fills a geometry given in geojson format with H3 hexagons at specified
    resolution. The flag_reverse_geojson allows to specify whether the geometry
    is lon/lat or swapped"""

    set_hexagons = h3.polyfill(geojson = geom_geojson,
                               res = res,
                               geo_json_conformant = flag_swap)
    list_hexagons_filling = list(set_hexagons)

    if flag_return_df is True:
        # make dataframe
        df_fill_hex = pd.DataFrame({"hex_id_10": list_hexagons_filling})
        df_fill_hex["AC Valor promedio"] = np.nan
        df_fill_hex["AT Valor promedio"] = np.nan
        df_fill_hex["Conteos"] = np.nan
        
        df_fill_hex['geometry'] = df_fill_hex.hex_id_10.apply(
                                    lambda x:
                                    {"type": "Polygon",
                                     "coordinates": [
                                        h3.h3_to_geo_boundary(h=x,
                                                              geo_json=True)
                                        ]
                                     })
        assert(df_fill_hex.shape[0] == len(list_hexagons_filling))
        return df_fill_hex
    else:
        return list_hexagons_filling
    

def mean_by_hexagon(df, res):
    """Aggregates the mean of AT at hexagon level"""

    col_hex_id = "hex_id_{}".format(res)
    col_geometry = "geometry_{}".format(res)
    col_geometry_to_plot_gpd = "geometry"

    # within each group preserve the first geometry and count the ids
    df_aggreg = df.groupby(by = col_hex_id).agg({col_geometry: "first",
                                                "AC_VM2": "mean",
                                                "AT_VM2": "mean",
                                                "LOTSECT_ID": "count"})

    df_aggreg.reset_index(inplace = True)
    df_aggreg.rename(columns={"AC_VM2": "AC Valor promedio"}, inplace = True)
    df_aggreg.rename(columns={"AT_VM2": "AT Valor promedio"}, inplace = True)
    df_aggreg.rename(columns={"LOTSECT_ID": "Conteos"}, inplace = True)

    df_aggreg.sort_values(by = ["AC Valor promedio", "AT Valor promedio"], ascending = False, inplace = True)
    df_aggreg[col_geometry_to_plot_gpd] = df_aggreg[col_geometry].apply(lambda p: shape(json.loads(json.dumps(p))))
    df_aggreg_geo = gpd.GeoDataFrame(df_aggreg, geometry=col_geometry_to_plot_gpd).set_crs('epsg:4326')
    
    return df_aggreg_geo



# Arreglar esta funcion para que reciba el parametro de salida y el nombre del archivo.
def make_gif(input_folder, output_file):
    """ Make a gif, this function takes all images in a folder and builds a .gif file"""
    pregif = []
    #time_per_step = 0.25
    for root, _, files in os.walk(input_folder):
        
        file_paths = [os.path.join(root, file) for file in files]
        file_paths = sorted(file_paths, key=lambda x: os.path.getmtime(x))
        pregif = [Image.open(mapa) for mapa in file_paths if mapa.endswith('.png')]
    pregif[0].save(output_file,
               save_all = True, append_images = pregif[1:], 
               optimize = False, loop=5, duration = 2000)
    
# for autocorrelation


def load_and_prepare_geojson(filepath):
    """Loads a geojson files of polygon geometries and features,
    swaps the latitude and longitude andstores geojson"""

    gdf_Polygons = gpd.read_file(filepath, driver="GeoJSON")
    
    gdf_Polygons['geometry']=gdf_Polygons.unary_union
    
    gdf_Polygons["geom_geojson"] = gdf_Polygons.geometry.apply(
                                              lambda x: geometry.mapping(x))

    gdf_Polygons["geom_swap"] = gdf_Polygons["geometry"].map(
                                              lambda polygon: ops.transform(
                                                  lambda x, y: (y, x), polygon))

    gdf_Polygons["geom_swap_geojson"] = gdf_Polygons["geom_swap"].apply(
                                              lambda x: geometry.mapping(x))
    
    return gdf_Polygons
    
