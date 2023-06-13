url_ciencuadras ="https://api.ciencuadras.com/api/realestates"
url_fincaraiz = "https://api.fincaraiz.com.co/document/api/1.0/listing/search?content-type=application/json"

raw_ciencuadras = '''{
    "status":false,"any":[],
"criteria":[{"transactionType":"arriendo"},
{"realEstateType":"apartamento"},
{},
{},
{},
{"IwantWith":[{"lift":false},
{"childrenArea":false},
{"surveillance":false},
{"deposit":false},
{"communalLiving":false},
{"fitnessCenter":false},
{"swimmingpool":false},
{"serviceRoom":false},
{"furnished":false},
{"greenZones":false},
{"allowsPets":false},
{"schoolsAndGardens":false},
{"urbanTransport":false},
{"malls":false},
{"parks":false},
{"supermarkets":false},
{"neighborhoodShops":false},
{"hospitals":false}]},
{},
{"orderBy":{}},
{},{},{},{},{},
{"proyect":0},
{},{},{},{},{},{},{},{},{},{},{},
{"locationGPS":{}},{},{},{},{},{},{},
{"hasFilter":false},
{"auction":0},
{"sorting":"num_parqueaderos"},
{"stratum":[]},{"builderName":[]},
{"foreignColombian":null},
{"intercambio":0},{}],
"pathurl":"arriendo/apartamento",
"numberPaginator":1}'''

raw_ini = '{"status":false,"any":[],"criteria":[{"transactionType":"arriendo"},{"realEstateType":"apartamento"},{},{},{},{"IwantWith":[{"lift":false},{"childrenArea":false},{"surveillance":false},{"deposit":false},{"communalLiving":false},{"fitnessCenter":false},{"swimmingpool":false},{"serviceRoom":false},{"furnished":false},{"greenZones":false},{"allowsPets":false},{"schoolsAndGardens":false},{"urbanTransport":false},{"malls":false},{"parks":false},{"supermarkets":false},{"neighborhoodShops":false},{"hospitals":false}]},{},{"orderBy":{}},{},{},{},{},{},{"proyect":0},{},{},{},{},{},{},{},{},{},{},{},{"locationGPS":{}},{},{},{},{},{},{},{"hasFilter":false},{"auction":0},{"sorting":"num_parqueaderos"},{"stratum":[]},{"builderName":[]},{"foreignColombian":null},{"intercambio":0},{}],"pathurl":"arriendo/apartamento","numberPaginator":%d}'
#arriendo
raw_arriendo = '{"status":false,"any":[],"criteria":[{"transactionType":"arriendo"},{},{},{},{},{"IwantWith":[{"lift":false},{"childrenArea":false},{"surveillance":false},{"deposit":false},{"communalLiving":false},{"fitnessCenter":false},{"swimmingpool":false},{"serviceRoom":false},{"furnished":false},{"greenZones":false},{"allowsPets":false},{"schoolsAndGardens":false},{"urbanTransport":false},{"malls":false},{"parks":false},{"supermarkets":false},{"neighborhoodShops":false},{"hospitals":false}]},{},{"orderBy":{}},{},{},{},{},{},{"proyect":0},{},{},{},{},{},{},{},{},{},{},{},{"locationGPS":{}},{},{},{},{},{},{},{"hasFilter":false},{"auction":0},{"sorting":"gimnasio"},{"stratum":[]},{"builderName":[]},{"foreignColombian":null},{"intercambio":0},{}],"pathurl":"arriendo","numberPaginator":1}'
raw_ini_arriendo = '{"status":false,"any":[],"criteria":[{"transactionType":"arriendo"},{},{},{},{},{"IwantWith":[{"lift":false},{"childrenArea":false},{"surveillance":false},{"deposit":false},{"communalLiving":false},{"fitnessCenter":false},{"swimmingpool":false},{"serviceRoom":false},{"furnished":false},{"greenZones":false},{"allowsPets":false},{"schoolsAndGardens":false},{"urbanTransport":false},{"malls":false},{"parks":false},{"supermarkets":false},{"neighborhoodShops":false},{"hospitals":false}]},{},{"orderBy":{}},{},{},{},{},{},{"proyect":0},{},{},{},{},{},{},{},{},{},{},{},{"locationGPS":{}},{},{},{},{},{},{},{"hasFilter":false},{"auction":0},{"sorting":"gimnasio"},{"stratum":[]},{"builderName":[]},{"foreignColombian":null},{"intercambio":0},{}],"pathurl":"arriendo","numberPaginator":%d}'
#Venta
raw_venta = '{"status":false,"any":[],"criteria":[{"transactionType":"venta"},{},{},{},{},{"IwantWith":[{"lift":false},{"childrenArea":false},{"surveillance":false},{"deposit":false},{"communalLiving":false},{"fitnessCenter":false},{"swimmingpool":false},{"serviceRoom":false},{"furnished":false},{"greenZones":false},{"allowsPets":false},{"schoolsAndGardens":false},{"urbanTransport":false},{"malls":false},{"parks":false},{"supermarkets":false},{"neighborhoodShops":false},{"hospitals":false}]},{},{"orderBy":{}},{},{},{},{},{},{"proyect":0},{},{},{},{},{},{},{},{},{},{},{},{"locationGPS":{}},{},{},{},{},{},{},{"hasFilter":false},{"auction":0},{"sorting":"num_banos"},{"stratum":[]},{"builderName":[]},{"foreignColombian":null},{"intercambio":0},{}],"pathurl":"venta","numberPaginator":1}'
raw_ini_venta = '{"status":false,"any":[],"criteria":[{"transactionType":"venta"},{},{},{},{},{"IwantWith":[{"lift":false},{"childrenArea":false},{"surveillance":false},{"deposit":false},{"communalLiving":false},{"fitnessCenter":false},{"swimmingpool":false},{"serviceRoom":false},{"furnished":false},{"greenZones":false},{"allowsPets":false},{"schoolsAndGardens":false},{"urbanTransport":false},{"malls":false},{"parks":false},{"supermarkets":false},{"neighborhoodShops":false},{"hospitals":false}]},{},{"orderBy":{}},{},{},{},{},{},{"proyect":0},{},{},{},{},{},{},{},{},{},{},{},{"locationGPS":{}},{},{},{},{},{},{},{"hasFilter":false},{"auction":0},{"sorting":"num_banos"},{"stratum":[]},{"builderName":[]},{"foreignColombian":null},{"intercambio":0},{}],"pathurl":"venta","numberPaginator":%d}'
#Proyectos nuevos
raw_Proyectos_Nuevos = '{"status":false,"any":[],"criteria":[{"transactionType":"venta"},{},{},{},{},{"IwantWith":[{"lift":false},{"childrenArea":false},{"surveillance":false},{"deposit":false},{"communalLiving":false},{"fitnessCenter":false},{"swimmingpool":false},{"serviceRoom":false},{"furnished":false},{"greenZones":false},{"allowsPets":false},{"schoolsAndGardens":false},{"urbanTransport":false},{"malls":false},{"parks":false},{"supermarkets":false},{"neighborhoodShops":false},{"hospitals":false}]},{},{"orderBy":{}},{},{},{},{},{},{"proyect":1},{},{},{"stageProyect":"0"},{},{},{},{},{},{},{},{},{"locationGPS":{}},{},{},{},{},{},{},{"hasFilter":false},{},{"sorting":"num_banos"},{"stratum":[]},{"builderName":[]},{"foreignColombian":null},{},{}],"pathurl":"proyectos-vivienda-nueva","numberPaginator":1}'
raw_ini_Proyectos_Nuevos = '{"status":false,"any":[],"criteria":[{"transactionType":"venta"},{},{},{},{},{"IwantWith":[{"lift":false},{"childrenArea":false},{"surveillance":false},{"deposit":false},{"communalLiving":false},{"fitnessCenter":false},{"swimmingpool":false},{"serviceRoom":false},{"furnished":false},{"greenZones":false},{"allowsPets":false},{"schoolsAndGardens":false},{"urbanTransport":false},{"malls":false},{"parks":false},{"supermarkets":false},{"neighborhoodShops":false},{"hospitals":false}]},{},{"orderBy":{}},{},{},{},{},{},{"proyect":1},{},{},{"stageProyect":"0"},{},{},{},{},{},{},{},{},{"locationGPS":{}},{},{},{},{},{},{},{"hasFilter":false},{},{"sorting":"num_banos"},{"stratum":[]},{"builderName":[]},{"foreignColombian":null},{},{}],"pathurl":"proyectos-vivienda-nueva","numberPaginator":%d}'

raw_fc_renta = '{"filter":{"offer":{"slug":["rent","sell"]},"property_type":{"slug":["apartment","studio","house","cabin","country-house","house-lot","farm","room","lot","warehouse","consulting-room","commercial","office","parking","building"]},"locations":{"location_point":[[-74.07072809421797,4.648984273791001],[-74.05995634281416,4.632537421167498]]}},"fields":{"exclude":[],"facets":["rooms.slug","baths.slug","locations.countries.slug","locations.states.slug","locations.cities.slug","locations.neighbourhoods.slug","locations.groups.slug","locations.groups.subgroups.slug","offer.slug","property_type.slug","categories.slug","stratum.slug","age.slug","media.floor_plans.with_content","media.photos.with_content","media.videos.with_content","products.slug","is_new"],"include":[],"limit":1073,"offset":0,"ordering":[],"platform":41,"with_algorithm":true}}'