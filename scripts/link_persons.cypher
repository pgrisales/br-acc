// ICARUS — Person Node SAME_AS Linking
// Creates SAME_AS relationships between Person nodes representing the same individual.
// Non-destructive: keeps separate nodes for source attribution, links them for traversal.
// Run once as a migration, then periodically after ETL reloads.

// ── Phase 0: Pre-compute cpf_middle6 on existing full-CPF Person nodes ──
// Strips formatting (XXX.XXX.XXX-XX → 11 digits), extracts middle 6 digits
// (positions [3:9]), stores as indexed property for partial-CPF matching.
MATCH (p:Person)
WHERE p.cpf IS NOT NULL AND p.cpf_middle6 IS NULL
WITH p, replace(replace(p.cpf, '.', ''), '-', '') AS digits
WHERE size(digits) = 11
SET p.cpf_middle6 = substring(digits, 3, 6);

// ── Phase 1: CPF match (confidence 0.95) ──────────────────────────
// TSE candidates that have unmasked CPF → CNPJ persons with same CPF.
// Both pipelines store formatted CPFs, so exact match is reliable.
MATCH (a:Person), (b:Person)
WHERE a.sq_candidato IS NOT NULL AND a.cpf IS NOT NULL
  AND b.cpf IS NOT NULL AND b.sq_candidato IS NULL
  AND a.cpf = b.cpf AND a <> b
MERGE (a)-[:SAME_AS {confidence: 0.95, method: "cpf_match"}]->(b);

// ── Phase 2: Author → TSE candidate by name (confidence 0.90) ────
// Transparencia/TransfereGov authors → TSE candidates.
// Both use normalize_name() from same transform module → exact match safe.
// Small set (~1K authors) vs medium set (TSE candidates).
MATCH (a:Person), (b:Person)
WHERE a.author_key IS NOT NULL AND b.sq_candidato IS NOT NULL
  AND a.name IS NOT NULL AND b.name IS NOT NULL
  AND a.name = b.name AND a <> b
MERGE (a)-[:SAME_AS {confidence: 0.90, method: "name_match_author_tse"}]->(b);

// ── Phase 3: Author → CNPJ person by name (confidence 0.80) ──────
// Transparencia/TransfereGov authors → CNPJ persons.
// Small set (~1K) vs large set (2M). Person(name) index required.
// Only links if no SAME_AS already exists between pair (avoids duplicates from Phase 2 chains).
MATCH (a:Person), (b:Person)
WHERE a.author_key IS NOT NULL AND b.cpf IS NOT NULL
  AND a.name IS NOT NULL AND b.name IS NOT NULL
  AND a.name = b.name AND a <> b
  AND NOT EXISTS { (a)-[:SAME_AS]-(b) }
MERGE (a)-[:SAME_AS {confidence: 0.80, method: "name_match_author_cnpj"}]->(b);

// ── Phase 4: Servidor partial CPF + name (confidence 0.95) ────────
// Servidores with LGPD-masked CPFs have 6 middle digits (positions [3:9]).
// Combined with exact name match, collision probability ~1 in 1 billion.
MATCH (s:Person), (p:Person)
WHERE s.cpf_partial IS NOT NULL
  AND p.cpf_middle6 IS NOT NULL
  AND s.cpf_partial = p.cpf_middle6
  AND s.name IS NOT NULL AND p.name IS NOT NULL
  AND s.name = p.name
  AND s <> p
  AND NOT EXISTS { (s)-[:SAME_AS]-(p) }
MERGE (s)-[:SAME_AS {confidence: 0.95, method: "partial_cpf_name_match"}]->(p);

// ── Phase 5: Classified servidores — unique name match (confidence 0.85) ──
// For ~34K servidores with blank CPF: match by name only when the name
// appears exactly once among blank-CPF servidores AND exactly once among
// full-CPF persons. Common names auto-excluded by size() != 1.
MATCH (s:Person)-[:RECEBEU_SALARIO]->(:PublicOffice)
WHERE s.cpf_partial IS NULL AND s.name IS NOT NULL
WITH s.name AS name, collect(s) AS servidores
WHERE size(servidores) = 1
WITH name, servidores[0] AS s
MATCH (p:Person)
WHERE p.cpf_middle6 IS NOT NULL
  AND p.name = name
  AND s <> p
  AND NOT EXISTS { (s)-[:SAME_AS]-(p) }
WITH s, collect(p) AS targets
WHERE size(targets) = 1
WITH s, targets[0] AS target
MERGE (s)-[:SAME_AS {confidence: 0.85, method: "unique_name_match_servidor"}]->(target);
