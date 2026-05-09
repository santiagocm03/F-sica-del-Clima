import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm

# 1. Definir los límites de los intervalos (clevs)
clevs = np.array([-1.0, -0.7, -0.5, -0.4, -0.3, -0.2, -0.1, -0.05, 0.0,
                   0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.7, 1.0])

# 2. Definir los colores en orden de uso (cc)
# Los colores 62-68 son verdes, 69 blanco (dos veces), 70-77 morados
colores_rgb = [
    # 62 - verde oscuro
    (0, 100, 0),
    # 63
    (0, 125, 0),
    # 64
    (50, 150, 50),
    # 65
    (100, 175, 100),
    # 66
    (150, 200, 150),
    # 67
    (200, 225, 200),
    # 68
    (220, 245, 220),
    # 69 - blanco (primer intervalo -0.05 a 0)
    (255, 255, 255),
    # 69 - blanco (segundo intervalo 0 a 0.05)
    (255, 255, 255),
    # 70
    (235, 235, 245),
    # 71
    (210, 205, 235),
    # 72
    (180, 170, 220),
    # 73
    (150, 130, 200),
    # 74
    (120, 95, 180),
    # 75
    (95, 60, 160),
    # 76
    (70, 30, 140),
    # 77
    (50, 10, 110)
]

# Convertir a valores normalizados 0-1
colores_norm = [(r/255, g/255, b/255) for r, g, b in colores_rgb]

# 3. Crear el colormap y la norma
cmap = ListedColormap(colores_norm)
norm = BoundaryNorm(clevs, len(colores_norm))

# 4. Generar la figura y la colorbar
fig, ax = plt.subplots(figsize=(10, 1.5))
fig.subplots_adjust(bottom=0.4)

# Crear un objeto mapeable (fake) para la colorbar
cb = fig.colorbar(
    plt.cm.ScalarMappable(norm=norm, cmap=cmap),
    cax=ax,
    orientation='horizontal',
    ticks=clevs,                  # Mostrar todos los niveles
    spacing='uniform',
    extend='neither'              # Sin extensiones (los extremos ya cubren -1 y 1)
)

# Formatear etiquetas: mostrar solo las más relevantes para evitar saturación
# Por ejemplo, mostrar cada dos o tres niveles. Personalizable.
ticks = clevs
# Si hay muchas etiquetas, puedes reducirlas:
ticks = clevs[::2]   # cada dos

cb.set_ticks(ticks)
# Opcional: formatear como string con 2 decimales
cb.set_ticklabels([f'{t:.2f}' for t in ticks])
cb.ax.tick_params(labelsize=15, rotation=0)

# Título (opcional)
#cb.set_label('Correlation coefficient', fontsize=10)

# Guardar o mostrar
plt.savefig('colorbar_horizontal.png', dpi=300, bbox_inches='tight')
plt.show()