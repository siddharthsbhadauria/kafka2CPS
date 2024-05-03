## Draw.io CSV format for Azure data transfer architecture.
## Node label with placeholders and HTML.
## Default is '%name_of_first_column%'.
#
# label: %name%<br><i style="color:gray;">%type%</i><br>
#
## Shapes and their styles
# stylename: type
# styles: {"onprem_server": "aspect=fixed;html=1;points=[];align=center;image;fontSize=15;image=img/lib/azure2/compute/Virtual_Machines.svg;",
# "data_factory": "aspect=fixed;html=1;points=[];align=center;image;fontSize=15;image=img/lib/azure2/integration/Data_Factory.svg;",
# "blob_storage": "aspect=fixed;html=1;points=[];align=center;image;fontSize=15;image=img/lib/azure2/storage/Storage_Accounts_Blob.svg;"}
## Connections between rows ("from": source column, "to": target column).
## Label, style and invert are optional.
#
# name, type, connect_to
"On-Premises Server","onprem_server","Data Factory"
"Azure Data Factory","data_factory","Blob Storage"
"Azure Blob Storage","blob_storage",""
