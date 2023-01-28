SELECT p.id, created_on, title, description, tag
FROM `mlops-text-classif.mlops_classif.projects` p
LEFT JOIN `mlops-text-classif.mlops_classif.tags` t
ON p.id = t.id