# Esquema do Armazém de Dados da ALMG

> Baseado na documentação oficial extraída do arquivo `armazem.pdf` (atualizado em 06/10/2025).

---

## 📁 Dimensões

### `dim_autor_emenda_proposicao`
Autores de emendas de proposições (deputados, comissões, governador, tribunais, etc.).  
**Relacionamentos:** 1:N com `fat_autoria_emenda_proposicao`  
**Colunas principais:**
- `sk_autor_emenda_proposicao` (PK)
- `id`, `nome`, `nome_completo`
- `tipo`, `grupo_autoria`
- `partido_atual`, `bloco_bancada_atual`, `bloco_bancada_sigla_atual`
- `legislatura_atual`, `em_exercicio`, `situacao_mandato`
- `naturalidade`, `data_nascimento`, `genero`
- `atividade_profissional`, `atuacao_parlamentar`, `vida_profissional_politica`, `condecoracoes`
- `url_foto`

---

### `dim_autor_proposicao`
Autores de proposições (mesmos registros de `dim_autor_emenda_proposicao`).  
**Relacionamentos:** 1:N com `fat_autoria_proposicao`  
**Colunas:** idênticas às de `dim_autor_emenda_proposicao`.

---

### `dim_primeiro_autor_proposicao`
Primeiro autor de proposições (mesmos registros das anteriores).  
**Relacionamentos:** 1:N com `dim_proposicao`  
**Colunas:** idênticas às de `dim_autor_emenda_proposicao`.

---

### `dim_comissao`
Comissões parlamentares cadastradas no SDP (ativas e inativas).  
**Relacionamentos:** 1:N com várias tabelas de fatos e dimensões  
**Colunas principais:**
- `sk_comissao` (PK)
- `id`, `nome`, `nome_reduzido`, `sigla`
- `tipo` (ex: Permanente, CPI)
- `data_inicio`, `data_termino`, `comissao_ativa`

---

### `dim_comissao_acao_reuniao` e `dim_comissao_distribuicao`
Variantes de `dim_comissao` para contextos específicos (ações em reunião e distribuições).  
**Colunas:** idênticas às de `dim_comissao`.

---

### `dim_data` e variantes (`dim_data_acao_reuniao_comissao`, `dim_data_protocolo_emenda_proposicao`, etc.)
Dimensão temporal com mais de 40.000 registros (01/01/1950 a 07/07/2059).  
**Colunas principais:**
- `sk_data` (PK)
- `ano_numero`, `mes_numero`, `dia_do_mes_numero`
- `dia_nome`, `mes_nome`, `trimestre_nome`
- `ano_mes_dia_desc` (ex: "2025-10-14")
- `legislatura`, `sessao_legislativa`, `sessao_legislativa_tipo`

> Existem 5 variantes especializadas:
> - `dim_data_acao_reuniao_comissao`
> - `dim_data_protocolo_emenda_proposicao`
> - `dim_data_protocolo_proposicao`
> - `dim_data_publicacao_proposicao`
> - `dim_data_recebimento_proposicao`

---

### `dim_deputado_estadual`
Deputados estaduais (ativos e inativos).  
**Relacionamentos:** 1:N com composições, votos, presenças  
**Colunas principais:**
- `sk_deputado_estadual` (PK)
- `id` (matrícula), `nome_parlamentar`, `nome_completo`
- `partido_atual`, `bloco_bancada_atual`
- `legislatura_atual`, `em_exercicio`, `situacao_mandato`
- `naturalidade`, `data_nascimento`, `url_foto`

---

### `dim_destinatario_diligencia`
Destinatários de diligências (deputados ou instituições).  
**Relacionamentos:** N:1 com `dim_proposicao`, `dim_instituicao`, `dim_deputado_estadual`, `dim_data`  
**Colunas principais:**
- `sk_destinatario_diligencia` (PK)
- `numero`, `oficios`, `sigla_instituicao`
- `situacao`, `tipo_destino`, `destino_interno`
- `qtd_oficios_emitidos`, `qtd_respostas`, `qtd_analises`
- `data_prazo`, `data_resposta_max`, `data_recebimento_max`

---

### `dim_destinatario_requerimento`
Destinatários de requerimentos.  
**Colunas principais:**
- `sk_destinatario_requerimento` (PK)
- `id_proposicao`, `id_instituicao`, `instituicao_nome`, `instituicao_sigla`
- `rqn_originado`, `rqcs_origem`
- `situacao`, `situacao_prazo`, `dias_ate_prazo`

---

### `dim_dispositivo_movel`
Dispositivos móveis de deputados (para votação remota).  
**Relacionamentos:** N:1 com `dim_deputado_estadual`  
**Colunas principais:**
- `sk_dispositivo_movel` (PK)
- `id`, `modelo`, `versao_app`, `versao_os`
- `ativo`, `habilitado_para_voto`, `data`, `data_inativacao`

---

### `dim_emenda_proposicao`
Emendas a proposições.  
**Relacionamentos:** 1:1 com fatos de emenda; 1:N com `fat_autoria_emenda_proposicao`  
**Colunas principais:**
- `sk_emenda_proposicao` (PK)
- `numeracao` (ex: "EMD 45 ao PL. 358/2023 T1")
- `tipo_descricao` (Emenda, Subemenda, Proposta de Emenda)
- `turno`, `descricao`, `indicador_recebimento`
- `autores`, `autores_principais`, `apoiadores`
- `de_turno_atual`

---

### `dim_evento_institucional`
Eventos institucionais cadastrados no SEA.  
**Relacionamentos:** 1:N com `dim_evento_legislativo`  
**Colunas principais:**
- `sk_evento_institucional` (PK)
- `nome`, `nome_evento_pai`, `ciclo`, `etapa`
- `data_inicio`, `data_termino`, `cancelado`, `situacao`
- `tema`, `tipo`, `tipo_etapa`, `url`, `url_logo`

---

### `dim_evento_legislativo`
Eventos legislativos (reuniões, visitas, audiências).  
**Relacionamentos:** N:1 com `dim_comissao`, `dim_data`, `dim_evento_institucional`  
**Colunas principais:**
- `sk_evento_legislativo` (PK)
- `nome`, `tipo`, `tipo_detalhado`, `tipo_reuniao`
- `data`, `local`, `municipio`, `uf_sigla`
- `realizado`, `cancelado`, `situacao`
- `categoria_fiscalizatoria`, `classificacao_evento_gco`, `url`

---

### `dim_grupo_parlamentar`
Grupos parlamentares (bancadas temáticas, frentes).  
**Relacionamentos:** 1:N com `fat_composicao_grupo_parlamentar`  
**Colunas principais:**
- `sk_grupo_parlamentar` (PK)
- `nome`, `tipo` (Bancada Temática / Frente Parlamentar)
- `data_inicio`, `data_termino`, `observacao`

---

### `dim_instituicao` e `dim_instituicao_remetente`
Instituições cadastradas no SEA (destinatárias ou remetentes).  
**Colunas principais:**
- `sk_instituicao` (PK)
- `nome`, `nome_abreviado`, `sigla`
- `tipo` (Pública / Privada), `municipio`, `uf`
- `esfera_publica` (Federal, Estadual, Municipal)

---

### `dim_lote`
Lotes de proposições criados no Silegis-MG (acesso restrito).  
**Colunas principais:**
- `sk_lote` (PK)
- `titulo`, `descricao`
- `setor_nome`, `setor_sigla`

---

### `dim_municipio`
Municípios cadastrados no Sistema de Localidades (SLO).  
**Colunas principais:**
- `sk_municipio` (PK)
- `id`, `nome`
- `pais_nome` ("Brasil"), `pais_sigla` ("BR")
- `uf_nome`, `uf_sigla`

---

### `dim_norma_juridica`
Normas jurídicas cadastradas no SNJ.  
**Relacionamentos:** 1:N com publicações, ADINs, indexações  
**Colunas principais:**
- `sk_norma_juridica` (PK)
- `numeracao`, `numero`, `ano`
- `tipo_sigla`, `tipo_descricao`
- `ementa`, `resumo`, `evento`, `apelido`
- `data`, `origem`, `situacao`, `vigencia_inicio`, `vigencia_termino`
- `categoria_norma`, `categoria_norma_gdi`, `categoria_norma_portal`
- `qtd_publicacoes`, `url`

---

### `dim_proposicao`
Proposições legislativas (PL, RQC, PEC, etc.).  
**Relacionamentos:** N:1 com autores, comissões, datas, deputados; 1:N com dezenas de fatos  
**Colunas principais:**
- `sk_proposicao` (PK)
- `id`, `numeracao`, `numero`, `ano`
- `tipo_sigla`, `tipo_descricao`
- `ementa`, `detalhamento_ementa`, `texto`
- `situacao_tramitacao`, `fase_tramitacao`, `em_tramitacao`
- `pronta_para_ordem_do_dia`, `regime_tramitacao`, `regime_urgencia`
- `autores`, `autores_principais`, `apoiadores`
- `categoria_proposicao`, `categoria_proposicao_portal`
- `norma_juridica`, `norma_juridica_descricao_completa`
- `url`, `url_silegis`

---

### `dim_proposicao_distribuicao_comissao`
Distribuições de proposições em comissões.  
**Colunas principais:**
- `sk_proposicao_distribuicao_comissao` (PK)
- `abrangencia`, `ativa`
- `data_hora_distribuicao`, `data_inicio_apreciacao`, `data_prazo_apreciacao`
- `ordem_apreciacao`, `parecer_concluido`, `turno`

---

### `dim_proposicao_lei`
Proposições de lei (PRL, PPC).  
**Colunas principais:**
- `sk_proposicao_lei` (PK)
- `tipo_sigla`, `tipo_descricao`
- `numero`, `ano`, `numeracao`
- `ementa`, `situacao`, `data`
- `norma`, `veto`, `oficio`, `documentos_sct`

---

### `dim_servidor` e `dim_setor`
Servidores e setores administrativos da ALMG.  
**Colunas principais:**
- `sk_servidor` / `sk_setor` (PK)
- `id`, `nome`, `nome_cracha` (servidor) / `sigla` (setor)

---

### Dimensões de Thesaurus (STH)

#### `dim_sth_completo`
Árvore completa do Thesaurus.  
- `sk_sth_completo` (PK)
- `caminho_completo`
- `termo01` a `termo13`

#### `dim_sth_thesaurus_tema`
Subárvore `/Almg/Thesaurus/Tema/`.  
- `sk_sth_thesaurus_tema` (PK)
- `caminho_completo`, `termo01` a `termo10`
- `categoria_proposicao`

#### `dim_sth_politicas_publicas`
Subárvore `/Almg/Políticas Públicas/Temas/`.  
- `sk_sth_politicas_publicas` (PK)
- `caminho_completo`, `termo01` a `termo10`

#### `dim_sth_comissoes_requerimentos`
Subárvore `/Almg/Comissões/Classificação de Requerimentos/`.  
- `sk_sth_comissoes_requerimentos` (PK)
- `caminho_completo`, `termo01` a `termo10`
- `tipo_evento`

#### `dim_sth_assembleia_fiscaliza`
Subárvore `/Almg/Comissões/Assembleia Fiscaliza/`.  
- `sk_sth_assembleia_fiscaliza` (PK)
- `caminho_completo`, `termo01` a `termo10`

#### `dim_sth_thesaurus_destinatarios`
Subárvore `/Almg/Thesaurus/Destinatários/`.  
- `sk_sth_thesaurus_destinatarios` (PK)
- `caminho_completo`, `termo01` a `termo10`

#### `dim_sth_municipio` e `dim_sth_thesaurus_tema_municipio`
Dados geográficos de municípios de MG para mapas.  
- `sk_sth_municipio` / `sk_sth_thesaurus_tema_municipio` (PK)
- `municipio`, `uf_sigla`, `pais_sigla`
- `microregiao_planejamento`, `regiao_planejamento` (última)

---

### `dim_reuniao_comissao`
Reuniões de comissão.  
**Colunas principais:**
- `sk_reuniao_comissao` (PK)
- `numero`, `data_hora`, `tipo` (Ordinária, Extraordinária, etc.)
- `duracao_em_minutos`, `desconvocada`, `ocorrida`
- `pauta_completa`, `resultado_completo`, `titulo`, `url`

---

### `dim_reuniao_plenario`
Reuniões de plenário.  
**Colunas principais:**
- `sk_reuniao_plenario` (PK)
- `numero`, `data_hora`, `tipo` (ORDINÁRIA, EXTRAORDINÁRIA, etc.)
- `duracao_em_minutos`, `desconvocada`
- `pauta_completa`, `resultado_completo`, `tipo_quorum`, `titulo`, `url`

---

## 📊 Fatos

### `fat_adin_norma_juridica`
Ações Diretas de Inconstitucionalidade contra normas jurídicas.  
- `sk_adin_norma_juridica` (PK)
- `numero`, `tipo`, `tribunal`, `dispositivo`
- `liminar`, `julgamento_merito`, `observacao`

---

### `fat_apreciacao_parecer_redacao_final_comissao` / `_plenario`
Aprovação de parecer de redação final em comissão ou plenário.  
- `sk_apreciacao...` (PK)
- `parecer_aprovado` (Sim/Não), `complemento`

---

### `fat_autoria_emenda_proposicao` / `fat_autoria_proposicao`
Vínculo entre autor e emenda/proposição.  
- `sk_autoria...` (PK)
- `ordenacao`, `autor_principal` (Sim/Não)

---

### `fat_comissao_evento_legislativo`
Participação de comissões em eventos legislativos.  
- `sk_comissao_evento_legislativo` (PK)
- `identificadora` (Sim/Não)

---

### `fat_composicao_comissao` / `fat_composicao_grupo_parlamentar`
Membros de comissões e grupos parlamentares.  
- `sk_composicao...` (PK)
- `cargo_atual`, `tipo` (Efetivo/Suplente), `data_inicio`, `data_termino`, `ativa`
- `papel` (Líder, Vice-Líder, etc.)

---

### `fat_convidado_reuniao_comissao` / `fat_participante_reuniao_comissao`
Convidados e participantes de reuniões de comissão.  
- `sk_convidado...` / `sk_participante...` (PK)
- `nome`, `cargo`, `instituicao`, `municipio`, `uf`
- `convocado`, `presenca_confirmada`, `presente`, `videoconferencia`

---

### `fat_destinatario_diligencia` / `fat_destinatario_requerimento` / `fat_destinatario_rqc`
Fatos agregados de destinatários.  
- `sk_destinatario...` (PK)
- `qtd_respostas`, `qtd_analises`, `qtd_oficios_emitidos`
- `data_resposta_max`, `data_recebimento_max`

---

### `fat_emenda_proposicao` / `fat_emenda_proposicao_turno_atual`
Fatos de emendas (todas ou apenas do turno atual).  
- `sk_emenda_proposicao` (PK)

---

### `fat_evento_legislativo`
Fato de evento legislativo.  
- `sk_evento_legislativo` (PK)
- `comissoes_reuniao_conjunta`, `qtd_proposicoes_pautadas`

---

### Vinculações de Proposições (`fat_mate_anexada`, `fat_mate_anexada_a`, `fat_mate_origem`, `fat_mate_vide`)
Proposições relacionadas por anexação, origem ou referência.  
- `sk_mate...` (PK)
- `numeracao`, `tipo_sigla`, `ementa`, `situacao_tramitacao`
- `pronta_para_ordem_do_dia`, `em_tramitacao`, `parecer_concluido`

---

### `fat_norma_juridica_sth_completo`
Indexação de normas na árvore completa do STH.  
- `sk_norma_juridica_sth_completo` (PK)

---

### `fat_presenca_deputado_reuniao_comissao` / `_plenario`
Presenças de deputados em reuniões.  
- `sk_presenca...` (PK)
- `tipo` (Efetivo, Suplente, etc.)
- `ades_tipo_presenca`, `tipo_registro` (para plenário)

---

### `fat_proposicao` (e variantes `fat_proposicao_sgm`, `fat_proposicao_gct`)
Proposições com metadados para apoio à pauta.  
- `sk_proposicao` (PK)
- `orientacao_para_pauta`, `elegivel_prepauta`
- `aj_tipo` (Constitucional/Inconstitucional), `at_impacto_politica_publica`
- `a_observacao_sgm` (apenas em `fat_proposicao_sgm`)

---

### `fat_proposicao_acao_reuniao_comissao` / `_plenario`
Ações em reuniões (aprovação, rejeição, adiamento, etc.).  
- `sk_proposicao_acao...` (PK)
- `tipo`, `semantica`, `sequencia`, `data`
- `ultima_acao` (Sim/Não)

---

### `fat_proposicao_agendamento_reuniao_comissao` / `_plenario`
Agendamentos em pauta.  
- `sk_proposicao_agendamento...` (PK)
- `ordem_apreciacao`, `turno`, `tipo_apreciacao`
- `faixa_constitucional`, `urgencia_tramitacao` (plenário)

---

### `fat_proposicao_analise`
Registros de análise técnica/jurídica.  
- `sk_proposicao_analise` (PK)
- `tipo` (Jurídica/Técnica), `observacao`
- `setor_sigla` (GCT), `subsetor_sigla` (GDC, GEC, etc.)

---

### `fat_proposicao_conteudo_documental`
Relacionamento entre proposição e conteúdo documental.  
- `sk_proposicao_conteudo_documental` (PK)

---

### `fat_proposicao_distribuicao_comissao`
Fato de distribuição em comissão.  
- `sk_proposicao_distribuicao_comissao` (PK)
- `abrangencia`, `turno`, `parecer_concluido`
- `data_hora_distribuicao`, `data_inicio_apreciacao`

---

### `fat_proposicao_lei`
Fato de proposição de lei.  
- `sk_proposicao_lei` (PK)
- Mesmas colunas de `dim_proposicao_lei`

---

### `fat_proposicao_proposicao_lei_norma_juridica`
Vinculação tríplice: proposição → proposição de lei → norma jurídica.  
- `sk_proposicao_proposicao_lei_norma_juridica` (PK)

---

### `fat_proposicao_relatoria_comissao`
Designações de relatoria em comissão.  
- `sk_proposicao_relatoria_comissao` (PK)
- `opiniao_parecer`, `complemento_parecer`
- `membro_deputado_comissao` (Efetivo/Suplente)
- `ativa`, `tipo_acao`, `semantica_acao`

---

### Fatos de Indexação no STH
- `fat_proposicao_sth_completo`
- `fat_proposicao_sth_thesaurus_tema`
- `fat_proposicao_sth_politicas_publicas`
- `fat_proposicao_sth_comissoes_requerimentos`
- `fat_proposicao_sth_assembleia_fiscaliza`
- `fat_proposicao_sth_thesaurus_destinatarios`
- `fat_proposicao_sth_municipio`
- `fat_proposicao_sth_thesaurus_tema_municipio`

> Todos seguem o padrão:
> - `sk_proposicao_sth_...` (PK)
> - Relacionamento N:1 com `dim_proposicao` e respectiva dimensão STH

---

### `fat_proposicao_tramitacao`
Histórico de tramitação.  
- `sk_proposicao_tramitacao` (PK)
- `data`, `local_descricao`, `local_sigla`, `texto`, `ordem`

---

### `fat_publicacao_norma_juridica`
Publicações de normas jurídicas.  
- `sk_publicacao_norma_juridica` (PK)
- `tipo` (Publicação, Errata, Republicação, etc.)
- `orgao`, `data`, `pagina`, `url_site_relacionado`

---

### `fat_resposta_destinatario_diligencia` / `_requerimento` / `_rqc`
Respostas a correspondências.  
- `sk_resposta...` (PK)
- `analise`, `sugestao_encaminhamento`, `requer_analise`
- `situacao_recebimento_comissao`, `situacao_recebimento_plenario`
- `tipo` (informações chegaram, providência tomada, etc.)

---

### `fat_rqc`
Fato de proposições do tipo RQC.  
- `sk_proposicao` (PK)

---

### `fat_rqc_evento`
Vinculação entre RQC e eventos legislativos.  
- `sk_rqc_evento` (PK)
- `nome`, `tipo`, `tipo_detalhado`, `situacao`, `data_evento`

---

### `fat_tarefa_silegis`
Tarefas do Silegis (acesso restrito a GDW_GTP).  
- `sk_tarefa_silegis` (PK)
- `nome`, `situacao`, `data_hora_criada`, etc.
- Relacionamentos com `dim_servidor` e `dim_setor`

---

### `fat_tema_norma_juridica`
Temas associados a normas jurídicas.  
- `sk_tema_norma_juridica` (PK)
- `descricao` (Administração Pública, Meio Ambiente, etc.)

---

### `fat_vide_norma_juridica`
Normas jurídicas que alteram outras normas.  
- `sk_vide_norma_juridica` (PK)
- `numeracao`, `tipo_sigla`, `artigos_alterados`, `comentario`
- `url`, `vigencia_inicio`, `vigencia_termino`

---

### `fat_vinculacao_deputado_proposicao`
Vinculações entre deputados e proposições (autoria, relatoria, desarquivamento).  
- `sk_vinculacao_deputado_proposicao` (PK)
- `tipo` (Autoria, Relatoria, Autoria de emenda, Desarquivamento)
- `data` (data do relacionamento)

---

### `fat_vinculacao_lote_proposicoes`
Composição de lotes de proposições.  
- `sk_vinculacao_lote_proposicoes` (PK)
- Acesso restrito a GDW_SGM e GDW_GCT

---

### `fat_vinculacao_proposicoes`
Vinculações genéricas entre proposições (para filtragem no Power BI).  
- `sk_vinculacao_proposicoes` (PK)
- Mesmas colunas das tabelas `fat_mate_*`

---

### `fat_voto_proposicao`
Votos individuais em plenário.  
- `sk_voto_proposicao` (PK)
- `opcao` (Sim, Não, Branco)
- `turno`, `categoria_apreciacao`, `descricao_votacao`
- `tipo_registro` (Oral, Remoto, Posto de votação)

---

> ✅ Este esquema reflete **todo o conteúdo** do documento `armazem.pdf` e está pronto para uso em documentação técnica, modelagem de BI ou desenvolvimento de queries SQL.