OOVS_DataScience
==============================

<img src="https://lh3.googleusercontent.com/drive-viewer/AJc5JmT807Awb7vJrjiYGsaFYQzP_RsXQ7OVzQML_SfvMAlx1Wcys-D_fJQ3Vnf_3R7paOFeiHhAJFQ=w1920-h912" alt="OOVS" width="450">

Este es el repositorio principal del OOVS, en este encuentra todos los flujos de captura, conexiones a datos y modelos de inteligencia artificial propuestos por el equipo o encargados del OOVS



🚀 El objetivo del observatorio de ocupación y valor del suelo, es monitorear y evaluar los efectos que tienen los proyectos de los corredores verdes de alta y media capacidad en la ciudad de Bogotá sobre las dinámicas de ocupación del suelo, las variables socioeconómicas y fluctuaciones en el valor de diferentes mercados del suelo e inmobiliarios en su área de influencia. Esto, con el fin de incidir y soportar la toma de decisiones relacionada con la formulación y seguimiento de las políticas públicas de estos corredores y su participación en la captura del valor generado en los diferentes mercados.
Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------

