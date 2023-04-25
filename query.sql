-- name: AddContainer :exec
INSERT INTO
	containers(id, name)
VALUES
	(
		?,
		?
	);

-- name: AddContainerAddr :exec
INSERT INTO
	addrs(addr, container_id)
VALUES
	(
		?,
		?
	);

-- name: AddContainerAlias :exec
INSERT INTO
	container_aliases(container_id, container_alias)
VALUES
	(
		?,
		?
	);

-- name: DeleteContainer :exec
DELETE FROM
	containers
WHERE
	id = ?;

-- name: GetContainers :many
SELECT 
	id,
	name
FROM
	containers;
