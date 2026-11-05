"""Gráficos de los descriptivos (matplotlib), siempre sobre resultados YA agregados.

Reglas de diseño:
- Una sola serie por gráfico -> un solo color (azul) y sin leyenda: el título la nombra.
- Barras horizontales delgadas: los nombres largos (entidades, modalidades) se leen bien.
- Línea gris punteada con la tasa global: es la referencia para decir "alto" o "bajo".
- El valor se escribe al final de cada barra, en color de texto (no en el color de la serie).
- Cuadrícula y ejes discretos para que el dato sea lo que destaca.
"""

COLOR_SERIE = "#2a78d6"
COLOR_TEXTO = "#0b0b0b"
COLOR_TEXTO_SECUNDARIO = "#52514e"
COLOR_SUPERFICIE = "#fcfcfb"


def _estilo(ax, titulo: str, etiqueta_x: str) -> None:
    ax.set_facecolor(COLOR_SUPERFICIE)
    ax.set_title(titulo, loc="left", color=COLOR_TEXTO, fontsize=12)
    ax.set_xlabel(etiqueta_x, color=COLOR_TEXTO_SECUNDARIO)
    ax.tick_params(colors=COLOR_TEXTO_SECUNDARIO, labelsize=9)
    for lado in ("top", "right", "left"):
        ax.spines[lado].set_visible(False)
    ax.spines["bottom"].set_color("#d9d8d4")
    ax.grid(axis="x", color="#ecebe7", linewidth=0.8)
    ax.set_axisbelow(True)


def barras_tasa(pdf, columna: str, tasa_global: float | None, titulo: str):
    """Barras horizontales de ``tasa_prorroga_pct`` por ``columna`` (DataFrame de pandas).

    ``pdf`` debe ser pequeño (resultado de ``tasa_por_grupo`` con ``limit``).
    Devuelve la figura para que el notebook la muestre o la guarde.
    """
    import matplotlib.pyplot as plt

    etiquetas = [str(v)[:45] for v in pdf[columna]]
    valores = list(pdf["tasa_prorroga_pct"])
    alto = 0.35 * len(valores) + 1.2
    fig, ax = plt.subplots(figsize=(8, alto), facecolor=COLOR_SUPERFICIE)
    posiciones = range(len(valores))
    ax.barh(posiciones, valores, height=0.6, color=COLOR_SERIE, zorder=2)
    ax.set_yticks(list(posiciones), etiquetas)
    ax.invert_yaxis()  # el mayor arriba
    maximo = max(valores) if valores else 0
    for y, v in zip(posiciones, valores, strict=True):
        # Fondo del color de la superficie: la etiqueta se lee aunque cruce la línea de referencia.
        ax.text(
            v + maximo * 0.01,
            y,
            f"{v:.1f} %",
            va="center",
            fontsize=8,
            color=COLOR_TEXTO,
            zorder=3,
            bbox={"facecolor": COLOR_SUPERFICIE, "edgecolor": "none", "pad": 1},
        )
    if tasa_global is not None:
        ax.axvline(tasa_global, color=COLOR_TEXTO_SECUNDARIO, linestyle="--", linewidth=1, zorder=1)
        ax.text(
            tasa_global,
            -0.8,
            f"tasa global {tasa_global:.1f} %",
            fontsize=8,
            color=COLOR_TEXTO_SECUNDARIO,
            ha="left",
        )
    ax.set_xlim(0, maximo * 1.15 if maximo else 1)
    _estilo(ax, titulo, "Tasa de prórroga (%)")
    fig.tight_layout()
    return fig


def linea_tasa(pdf, columna_x: str, titulo: str, etiqueta_x: str = "Mes de firma"):
    """Línea de ``tasa_prorroga_pct`` en el tiempo (p. ej. por mes de firma)."""
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(9, 3.5), facecolor=COLOR_SUPERFICIE)
    ax.plot(pdf[columna_x], pdf["tasa_prorroga_pct"], color=COLOR_SERIE, linewidth=2)
    ax.plot(pdf[columna_x], pdf["tasa_prorroga_pct"], "o", color=COLOR_SERIE, markersize=4)
    ax.set_ylim(bottom=0)
    ax.set_ylabel("Tasa de prórroga (%)", color=COLOR_TEXTO_SECUNDARIO)
    _estilo(ax, titulo, etiqueta_x)
    ax.grid(axis="y", color="#ecebe7", linewidth=0.8)
    ax.tick_params(axis="x", labelrotation=90)
    fig.tight_layout()
    return fig
