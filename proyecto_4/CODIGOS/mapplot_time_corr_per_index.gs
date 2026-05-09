*=========================================================
* Map plotting script for Correlation Maps (-1 to 1)
*=========================================================

function main(args)

file    = subwrd(args,1)
outfile = subwrd(args,2)
index   = subwrd(args,3)
varname = subwrd(args,4)

'reinit'

say 'Processing file: 'file
say 'Index: 'index
say 'NC variable: 'varname

*-----------------------------------
* Open dataset
*-----------------------------------

'sdfopen 'file

*-----------------------------------
* Remove watermarks
*-----------------------------------

'set mproj scaled'
'set timelab off'

*-----------------------------------
* Correlation bounds (-1 to 1)
* More resolution near zero
*-----------------------------------

* ========= CAR =========
if (index = 'car' | index = 'CAR')
  cl='-1 -0.7 -0.5 -0.4 -0.3 -0.2 -0.1 -0.05 0 0.05 0.1 0.2 0.3 0.4 0.5 0.7 1'
  cc='62 63 64 65 66 67 68 69 69 70 71 72 73 74 75 76 77'
endif

* ========= NINO3 =========
if (index = 'nino3' | index = 'NINO3')
  cl='-1 -0.7 -0.5 -0.4 -0.3 -0.2 -0.1 -0.05 0 0.05 0.1 0.2 0.3 0.4 0.5 0.7 1'
  cc='62 63 64 65 66 67 68 69 69 70 71 72 73 74 75 76 77'
endif

* ========= NINO3.4 =========
if (index = 'nino3.4' | index = 'nino34')
  cl='-1 -0.7 -0.5 -0.4 -0.3 -0.2 -0.1 -0.05 0 0.05 0.1 0.2 0.3 0.4 0.5 0.7 1'
  cc='62 63 64 65 66 67 68 69 69 70 71 72 73 74 75 76 77'
endif

* ========= NTA =========
if (index = 'nta' | index = 'NTA')
  cl='-1 -0.7 -0.5 -0.4 -0.3 -0.2 -0.1 -0.05 0 0.05 0.1 0.2 0.3 0.4 0.5 0.7 1'
  cc='62 63 64 65 66 67 68 69 69 70 71 72 73 74 75 76 77'
endif

* ========= SOI =========
if (index = 'soi' | index = 'SOI')
  cl='-1 -0.7 -0.5 -0.4 -0.3 -0.2 -0.1 -0.05 0 0.05 0.1 0.2 0.3 0.4 0.5 0.7 1'
  cc='62 63 64 65 66 67 68 69 69 70 71 72 73 74 75 76 77'
endif

* ========= TNA =========
if (index = 'tna' | index = 'TNA')
  cl='-1 -0.7 -0.5 -0.4 -0.3 -0.2 -0.1 -0.05 0 0.05 0.1 0.2 0.3 0.4 0.5 0.7 1'
  cc='62 63 64 65 66 67 68 69 69 70 71 72 73 74 75 76 77'
endif

*-----------------------------------
* Display configuration
*-----------------------------------

'set display color white'
'c'

'set gxout shaded'
'set mpdset hires'
'set csmooth off'

*-----------------------------------
* PuOr-like Balanced Palette (9 + 1 + 9)
*-----------------------------------

* --- Negative (dark → light green) ---
'set rgb 60 0 50 0'      ;* verde muy oscuro
'set rgb 61 0 75 0'
'set rgb 62 0 100 0'
'set rgb 63 0 125 0'
'set rgb 64 50 150 50'
'set rgb 65 100 175 100'
'set rgb 66 150 200 150'
'set rgb 67 200 225 200'
'set rgb 68 220 245 220'  ;* verde muy claro

* --- ZERO (pure white) ---
'set rgb 69 255 255 255'

* --- Positive (light → dark purple/blue) ---
'set rgb 70 235 235 245'
'set rgb 71 210 205 235'
'set rgb 72 180 170 220'
'set rgb 73 150 130 200'
'set rgb 74 120 95 180'
'set rgb 75 95 60 160'
'set rgb 76 70 30 140'
'set rgb 77 50 10 110'
'set rgb 78 30 0 80'

*-----------------------------------
* Apply color scale
*-----------------------------------

'set clevs 'cl
'set ccols 'cc

*-----------------------------------
* Axis formatting
*-----------------------------------

'set xlopts 1 8 0.20'
'set ylopts 1 8 0.20'
'set xlint 12'
'set ylint 5'

* Área de dibujo ajustada a la página (alto máximo 8.5)
'set parea 2.5 9.0 0.6 8.3'

'set grads off'

* --- RECORTAR REGIÓN GEOGRÁFICA ---
'set lon -84 -32.5'
'set lat -56.5 15'

*-----------------------------------
* Expression
*-----------------------------------

expr = 'smth9('varname')'

say 'Plotting expression: 'expr

'd 'expr

*-----------------------------------
* Overlay shapefile
*-----------------------------------

*'draw shp /home/santiago/Escritorio/UNIVERSIDAD/FISICA_DEL_CLIMA/F-sica-del-Clima/proyecto_4/DATOS'

*-----------------------------------
* Save
*-----------------------------------

'printim 'outfile' png'

say 'Finished.'

'quit'

return