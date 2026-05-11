# Brasoes dos Municipios do Brasil

Coletor e acervo de brasoes municipais brasileiros a partir de dados do Wikidata/Wikimedia Commons.

## Arquivos

- `brasoes/`: ~3600 imagens já coletadas.
- `brasoes/download-state.json`: estado incremental dos downloads, indexado por `COD_IBGE`, `UF` e `MUNICIPIO`.
- `main.js`: script de coleta, retry, retomada e conversao para PNG.

## Coleta

```bash
npm install
npm start
```

O script evita baixar novamente arquivos ja registrados no estado ou ja existentes como PNG. Downloads usam delay entre requisicoes, retry com backoff exponencial e conversao para PNG via `sharp`.

## Fonte

As URLs das imagens vem do Wikidata e apontam para arquivos do Wikimedia Commons. Verifique a pagina original de cada arquivo para detalhes de autoria e licenca.
