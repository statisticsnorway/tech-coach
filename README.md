# tech-coach

Testprosjekt for tech-coacher på seksjon 703 IT-partner.

## Utviklerdokumentasjon

Vi bruker [poetry](https://python-poetry.org/docs/) til pakkehåndtering. Pakkene blir
definert i fila `pyproject.toml`, og den konkrete versjonen ligger i fila
`poetry.lock`. Når man har klonet ut et nytt repo, må man opprette et virtuelt miljø
med disse pakkene. Det gjør man med kommandoen `poetry install`.

### Pre-commit hooks

Pre-commit hooks er sjekker git kjører rett etter at du har skrevet `git commit`, men
før git gjennomfører comitten. De kan hjelpe deg til å sikre at at comittene ikke
inneholder noe de ikke skal, at de er formattert riktig osv.

Det anbefales at alle som utvikler på repoet aktiverer pre-commit hooks.

Pre-commit hooks gjelder per repo, og må aktiveres etter at man har klonet ut repoet.
Det gjør du ved å gå til repo-katalogen og skriv kommandoen:

```bash
poetry run pre-commit install
```

Deretter vil den kjøre sjekkene som er beskrevet i fila `.pre-commit-config.yaml` hver
gang du comitter. Du kan også kjøre sjekkene manuelt ved å bruke kommandoen:

```bash
poetry run pre-commit run --all-files
```

Hvis noen av pre-commit sjekkene feiler, så prøv å kjøre pre-commit kommandoen på nytt.
Den fikser som regel det meste, men må kjøres på nytt for å gå feilfritt gjennom med de
korrigerte filene.

### Jupyter Notebooks

The files ending with `_ipynb.py` are jupyter notebooks
stored as plain python files, using `jupytext`. To open them as Jupyter notebooks,
right-click on them in JupyterLab and select Open With &rarr; Notebook.

When testing locally, start JupyterLab with this command:

```shell
poetry run jupter lab
```

For VS Code there are extensions for opening a python script as Jupyter Notebook,
for example:
[Jupytext for Notebooks](https://marketplace.visualstudio.com/items?itemName=donjayamanne.vscode-jupytext).
