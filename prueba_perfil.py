# -*- coding: utf-8 -*-
from PLANIFICADOR_ABC import (
    cargar_grado_A, cargar_grado_B, cargar_grado_C, construir_mapas
)
from perfil_competencias import (
    perfil_competencias, exportar_perfil_competencias
)

# 1) Cargar datos y mapas
dfA = cargar_grado_A()
dfB = cargar_grado_B()
dfC = cargar_grado_C()
map_b_a, map_c_b, ref_b, ref_c = construir_mapas(dfA, dfB, dfC)

# 2) Marca aquí lo que la persona ya tiene (pon tus códigos reales)
mis_A = {"ADG_A_0156_01", "ADG_A_0156_02"}   # ejemplo
mis_B = {"AFD_B_3003"}                       # ejemplo
mis_C = set()                                # si tienes C cursados, añádelos

# 3) Calcula perfil y expórtalo a Excel
out = exportar_perfil_competencias(mis_A, mis_B, mis_C, dfA, dfB, dfC, map_b_a,
                                   out_xlsx="PERFIL_COMPETENCIAS_EJEMPLO.xlsx")
print("OK ->", out)
