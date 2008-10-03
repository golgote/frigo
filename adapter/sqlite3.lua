-------------------------------------------------------------------------------
-- Frigo is a simple ORM working on top of LuaSQL.
--
-- @author Bertrand Mansion (bmansion@mamasam.com)
-- @copyright 2008 Bertrand Mansion
-------------------------------------------------------------------------------

module("frigo.adapter.sqlite3", package.seeall)

-- Meta information
_COPYRIGHT = "Copyright (C) 2008 Bertrand Mansion"

driver = "sqlite3"

local mappings = {
  char 		  = 'string',
  varchar 	= 'string',
  tinytext 	= 'string',
  text 		  = 'string',
  mediumtext= 'string',
  longtext 	= 'string',
  clob 		  = 'string',
  int 		  = 'integer',
  integer 	= 'integer',
  tinyint 	= 'integer',
  smallint 	= 'integer',
  mediumint = 'integer',
  bigint 	  = 'integer',
  year 		  = 'integer',
  float 	  = 'float',
  decimal 	= 'string',
  double 	  = 'float',
  blob		  = 'binary',
  tinyblob	= 'binary',
  mediumblob= 'binary',
  longblob	= 'binary',
  binary	  = 'binary',
  varbinary	= 'binary',
  date 		  = 'date',
  time		  = 'time',
  datetime	= 'datetime',
  timestamp	= 'datetime'
}

function identifier(self, str)
  return '"'..str:gsub('"', '""')..'"'
end

function escape(self, str)
  return self.conn:escape(str)
end

function tablelist(self)
  if self.listcache then
    return self.listcache
  end
  self.listcache = {}
  local cur = self.conn:execute"SELECT name FROM sqlite_master WHERE type = 'table'"
  local row = cur:fetch({})
  while row do
    self.listcache[row[1]] = true
    row = cur:fetch(row)
  end
  cur:close()
  return self.listcache
end

function tableinfo(self, tablename)
  if self.infocache[tablename] then 
    return self.infocache[tablename]
  end
	local cols = {}
	local pk = {}
  local autoinc = false

	local cur = assert(self.conn:execute("PRAGMA table_info("..self:identifier(tablename)..")"),
	  "table '" .. tablename .. "' not found");
  local row = cur:fetch({}, "a")
  while row do

    local field = {}
    field.null = row.notnull ~= 99 and true or false
    field.column = row.name
    if row.pk == 1 then
      table.insert(pk, row.name)
      field.key = true
    end

    local type, length, precision = string.match(string.lower(row.type), "(%w+)%(?(%d*),?(%d*)%)?")

    field.data_type = mappings[type];
    field.type = type
    field.length = length ~= "" and tonumber(length) or nil
    field.precision = precision ~= "" and tonumber(precision) or nil
    field.default = self:cast(field.data_type, row.dflt_value)
    field.tablename = tablename
    if string.lower(row.type) == "integer" and row.pk == 1 then
      autoinc = true
    end
    table.insert(cols, field)
    row = cur:fetch(row, "a")
  end
  cur:close()
  self.infocache[tablename] = {cols = cols, pk = pk, autoinc = autoinc}
  return self.infocache[tablename]
end

function limitQuery(self, q, from, count, ...)
  if not from then
    from = 0
  end
  if from >= 0 and count and count > 0 then
    return q .. " LIMIT " .. from .. " OFFSET " .. count
  end
  if from > 0 and not count then
    count = from
    return q .. " LIMIT " .. count .. " OFFSET 0"
  end
  return q
end

function startTransaction(self)
  self.conn:execute("BEGIN")
  return {
    conn = self.conn,
    commit = function(self)
      self.conn:execute("END")
    end,
    rollback = function(self)
      self.conn:execute("ROLLBACK")
    end
  }
end
