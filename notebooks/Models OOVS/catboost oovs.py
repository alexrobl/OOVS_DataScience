# Databricks notebook source
# MAGIC %md
# MAGIC Created on June 27 - 2023 </br>
# MAGIC @author: Alexander Robles

# COMMAND ----------

# MAGIC %md
# MAGIC Clasificador de descripciones de inmuebles a etiqueta, esto es necesario en vista de que algunos inmuebles son publicados con una descripcion diferente a la etiqueta puesta por el vendedor, esto genera errores a la hora de identificar un posible valor del tipo de inmueble.

# COMMAND ----------

import numpy as np
import pandas as pd
import os
import missingno as msno
from sklearn import preprocessing
from catboost import CatBoostClassifier, Pool
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import roc_auc_score, accuracy_score

# COMMAND ----------

df = pd.read_csv('/dbfs/FileStore/tables/procesado/BTA_INMO_IQR_FEBRERO_2023__2_.csv')

# COMMAND ----------

df2 = df.copy()
df2.drop(columns=['Unnamed: 0','index','id','nombre_usuario','codigo','edad','fecha_modificacion','canon_arrendamiento','fecha_creacion','localidad','estrato','barrio','contacto_llamada', 'id_usuario', 'url_encode', 'Latitud', 'Longitud','codigo_manzana', 'estratoCarto', 'dist','estratoAjustado','Estrato_name', 'tipo_usuario', 'nombre_usuario_2','fecha_expiracion', 'fecha_publicado', 'piso', 'Localizacion_Localidades', 'Localizacion_Comuna', 'Localizacion_Zona', 'Localizacion_Region', 'index_right', 'periodo', 'titulo', 'hex_id_8', 'geometry_8', 'hex_id_9', 'geometry_9', 'hex_id_10', 'geometry_10', 'group_inde', 'Tiempo','layer', 'Linea', 'url_inmueble','valor_administracion','Valor_M2','precio','direccion','geometry','tipo_transaccion',
'area', 'num_banos', 'num_habitaciones', 'num_parqueaderos',], inplace=True)
df2 = df2[~df2.descripcion.isna()].reset_index()


# COMMAND ----------

#msno.heatmap(df2)

# COMMAND ----------

# dado que los valores nulos se encuentran en el numero de baños, habitaciones y parqueaderos podemos suponer que son ero, en tal caso completo el set de datos faltantes.
df2.fillna(0, inplace=True)
msno.matrix(df2, color=(0.27, 0.52, 1.0))

# COMMAND ----------

df2.columns

# COMMAND ----------

from sklearn.model_selection import train_test_split
X = df2.copy()
y = X.pop(df2.columns[1])

# COMMAND ----------

# Separar los datos en datos de entrenamiento y testing
X_train, X_test, y_train, y_test = train_test_split( X, y, test_size=0.3, random_state=2)

# COMMAND ----------

def label_encoding(train: pd.DataFrame, test: pd.DataFrame, col_definition: dict):
    """
    col_definition: encode_col
    """
    n_train = len(train)
    train = pd.concat([train, test], sort=False).reset_index(drop=True)
    for f in col_definition['encode_col']:
        try:
            lbl = preprocessing.LabelEncoder()
            train[f] = lbl.fit_transform(list(train[f].values))
        except:
            print(f)
    test = train[n_train:].reset_index(drop=True)
    train = train[:n_train]
    return train, test

# COMMAND ----------

categorical_features = ['uso'] 
text_cols = ['descripcion']

# COMMAND ----------

X_train, X_test = label_encoding(X_train, X_test, col_definition={'encode_col': categorical_features})

# COMMAND ----------

catboost_params = {
    'iterations': 1000,
    'learning_rate': 0.1,
    'early_stopping_rounds': 10,
    'verbose': 100,
    'text_features': text_cols

}

from catboost import CatBoostClassifier
classifier = CatBoostClassifier(**catboost_params)
classifier.fit(X_train, y_train)

# COMMAND ----------

from sklearn.metrics import confusion_matrix, accuracy_score
y_pred = classifier.predict(X_test)
cm = confusion_matrix(y_test, y_pred)
print(cm)
accuracy_score(y_test, y_pred)

# COMMAND ----------

from sklearn.metrics import classification_report

print(classification_report(y_test, y_pred))

# COMMAND ----------

y.value_counts()

# COMMAND ----------

import pickle
pickle.dump(classifier, open("../../models/text_inmutipo_classificator.pk", "wb"))
