# Flujo de trabajo con Git y GitHub

## 1. Ramas

| Rama | Para qué | Quién escribe |
|---|---|---|
| `main` | Versión estable, la que se sustenta. Solo recibe cambios por pull request. | Nadie directamente |
| `trabajo/<tema>` | Una rama por tarea (ej. `trabajo/notebook-plata`, `trabajo/informe`). | Quien hace la tarea |

**Por qué:** si cada uno trabaja en su rama, los cambios a medias de uno no rompen
el trabajo del otro, y `main` siempre funciona.

```bash
git switch main
```
```bash
git pull origin main
```
```bash
git switch -c trabajo/notebook-plata
```

Línea por línea: (1) vuelvo a `main`; (2) traigo lo último de GitHub; (3) creo mi
rama a partir de ese `main` actualizado (`-c` = crear).

## 2. Commits específicos

Un commit = un cambio con un propósito. El mensaje dice **qué** cambia y, si no es
obvio, **por qué**.

| Bien | Mal |
|---|---|
| `Plata: elegir el nombre más largo cuando una entidad tiene dos` | `cambios` |
| `Oro: excluir dias_adicionados nulos de la etiqueta` | `arreglos varios` |
| `Docs: runbook con error de partición anio_carga` | `update` |

```bash
git add src/secop/plata.py tests/test_capas.py
```
```bash
git commit -m "Plata: elegir el nombre más largo cuando una entidad tiene dos"
```

Se agregan archivos **por nombre** (no `git add .` a ciegas) para no subir por
accidente datos o credenciales. El `.gitignore` es la segunda barrera.

## 3. Regla de oro: `pull` antes de `push`

Antes de subir, siempre traer lo que el compañero subió:

```bash
git pull --rebase origin trabajo/notebook-plata
```
```bash
git push -u origin trabajo/notebook-plata
```

**Por qué:** si JP subió algo a la misma rama y Cristian hace `push` sin `pull`,
GitHub rechaza el `push` (o, peor, alguien fuerza y borra trabajo ajeno).
`--rebase` pone mis commits encima de los del compañero y deja un historial lineal.
Si hay conflicto, Git marca las líneas en disputa: se editan, `git add` y
`git rebase --continue`.

**Nunca** usar `git push --force` en ramas compartidas.

## 4. Pull request (PR)

1. Subir la rama y abrir un PR hacia `main` en GitHub.
2. La plantilla (`.github/pull_request_template.md`) pide qué cambia, cómo se probó
   y una lista de control (datos personales, fuga, credenciales...).
3. GitHub Actions ejecuta automáticamente `py_compile`, `ruff`, la validación del
   esquema, `pytest` y la imagen de Docker. El PR muestra ✅ o ❌.
4. El compañero revisa y aprueba; luego se hace *merge*.

**Por qué:** dos ojos ven más que uno, y la CI evita que llegue a `main` código
que no compila o rompe las pruebas.

## 5. Etiquetas (tags) de versión

Cuando `main` llega a un punto importante (por ejemplo, "pipeline completo
ejecutado"), se marca con una etiqueta:

```bash
git tag -a v0.1 -m "v0.1: bronce, plata y oro ejecutados en Databricks"
```
```bash
git push origin v0.1
```

`-a` crea una etiqueta *anotada* (con autor, fecha y mensaje). La versión del
paquete en `src/secop/__init__.py` (`__version__`) debe coincidir, porque se
registra en MLflow: así cada modelo queda ligado a una versión exacta del código.

Plan de versiones sugerido (las etiquetas se crean cuando se cumpla cada hito):

| Etiqueta | Hito |
|---|---|
| `v0.1` | Bronce, plata y oro ejecutados en Databricks |
| `v0.2` | Descriptivo y modelo ejecutados; resultados en `docs/informe.md` |
| `v1.0` | Versión final sustentada |

## 6. Databricks Git folders

El repositorio se enlaza en Databricks como *Git folder* (ver
`docs/runbook_databricks.md`). Desde el menú Git del folder se hace `pull` para
traer cambios y se puede hacer `commit` y `push`. La regla de oro aplica igual:
**pull antes de editar y antes de push**.
