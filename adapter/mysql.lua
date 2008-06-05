-------------------------------------------------------------------------------
-- Frigo is a simple ORM working on top of LuaSQL.
--
-- @author Bertrand Mansion (bmansion@mamasam.com)
--
-- @copyright 2008 Bertrand Mansion
-- @release $Id: $
-------------------------------------------------------------------------------

module("frigo.adapter.mysql", package.seeall)

-- Meta information
_COPYRIGHT = "Copyright (C) 2008 Bertrand Mansion"

driver = "mysql"

local mappings = {
  char 		  = 'string',
  varchar 	= 'string',
  tinytext 	= 'string',
  enum 		  = 'string',
  set 		  = 'string',
  text 		  = 'string',
  mediumtext= 'string',
  longtext 	= 'string',
  clob 		  = 'string',
  int 		  = 'integer',
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
  return "`"..str:gsub("`", "``").."`"
end

function escape(self, str)
  return self.conn:escape(str)
end

function tablelist(self)
  local tables = {}
  local cur = self.conn:execute"SHOW TABLES"
  local row = cur:fetch({})
  while row do
    -- reusing the table of results
    table.insert(tables, row[1])
    row = cur:fetch(row)
  end
  cur:close()
  return tables
end

function tableinfo(self, tablename)
	local cols = {}
	local pk = {}
  local autoinc = false

	local cur = self.conn:execute("DESCRIBE ".. self:identifier(tablename));
  local row = cur:fetch({}, "a")
  while row do

    local field = {}
    field.null = row.Null == 'YES' and true or false
    field.column = row.Field
    if row.Key == 'PRI' then
      table.insert(pk, row.Field)
    end

    local type, length, precision = string.match(row.Type, "(%w+)%(?(%d*),?(%d*)%)?")
    field.data_type = mappings[type];
    field.type = type
    field.length = length ~= "" and tonumber(length) or nil
    field.precision = precision ~= "" and tonumber(precision) or nil
    field.default = row.Default
    if row.Extra == "auto_increment" and row.Key == "PRI" then
      autoinc = true
    end
    table.insert(cols, field)
    row = cur:fetch(row, "a")
  end
  cur:close()
  return {cols = cols, pk = pk, autoinc = autoinc}
end

function limitQuery(self, q, from, count, ...)
  if not from then
    from = 0
  end
  if from >= 0 and count and count > 0 then
    return q .. " LIMIT " .. from .. ", " .. count
  end
  if from > 0 and not count then
    count = from
    return q .. " LIMIT " .. count
  end
  return q
end

function startTransaction(self)
  self.conn:execute("START TRANSACTION")
  return {
    conn = self.conn,
    commit = function(self)
      self.conn:execute("COMMIT")
    end,
    rollback = function(self)
      self.conn:execute("ROLLBACK")
    end
  }
end
