-------------------------------------------------------------------------------
-- Frigo is a simple ORM working on top of LuaSQL.
--
-- @author Bertrand Mansion (bmansion@mamasam.com)
-- @copyright 2008 Bertrand Mansion
-------------------------------------------------------------------------------

module('frigo.relation', package.seeall)

_COPYRIGHT = "Copyright (C) 2008 Bertrand Mansion"
_DESCRIPTION = "Frigo DB is a simple wrapper for common LuaSQL manipulations."
_VERSION = "0.0.1"

function link(self, from, to)
  if not self.pivot then
    local cols = self.joins[from.__table]
    for a,b in pairs(cols) do
      from{[a] = to{b}}
    end
    from:freeze()
  end
end

function prepare(self, from, table2, options, values)
  local using = ""
  local table1 = from.__table
  local join = {}

  -- todo : escape tablename 
  
  -- using

  if self.pivot then
    -- n:n
    
    
  else
    local cols = self.joins[table1]
    for k,v in pairs(cols) do
      local j = table1 .. "." .. k .. " = " .. table2 .. "." .. v
      table.insert(join, j)
    end
    using = from.__db:identifier(table1) .. " INNER JOIN " .. 
      from.__db:identifier(table2) .. " ON " ..
      "(" .. table.concat(join, " AND ") .. ")"
  end

  if options.using then
    options.using = options.using .. using
  else
    options.using = using
  end
  
  -- where
  local where = {}
  local primaryKey = from:info().pk
  for _, pk in ipairs(primaryKey) do
    where[#where+1] = table1 .. "." .. pk .. " = ?"
    values[#values+1] = from:value(pk)
  end
  if options.where then
    options.where = table.concat(where, " AND ") .. " AND " .. options.where
  else
    options.where = table.concat(where, " AND ")
  end
end

function new(table1, table2, relation)
  local obj = {}
  obj.pivot = relation.pivot or nil
  obj.cascade = relation.cascade or false
  obj.joins = {}

  for from,to in pairs(relation.join) do
    if not obj.joins[table1] then
      obj.joins[table1] = {}
    end
    if not obj.joins[table2] then
      obj.joins[table2] = {}
    end
    obj.joins[table1][from] = to
    obj.joins[table2][to] = from
  end
  setmetatable(obj, {__index = _M})
  return obj
end