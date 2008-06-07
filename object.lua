-------------------------------------------------------------------------------
-- Frigo is a simple ORM working on top of LuaSQL.
--
-- @author Bertrand Mansion (bmansion@mamasam.com)
-- @copyright 2008 Bertrand Mansion
-------------------------------------------------------------------------------

module("frigo.object", package.seeall)

-- Meta information
_COPYRIGHT = "Copyright (C) 2008 Bertrand Mansion"
_DESCRIPTION = "Frigo is a simple ORM working on top of LuaSQL"
_VERSION = "0.0.1"

function set(self, colname, value)
  local col = self:colinfo(colname)
  if col then
    if self[colname] then
      if type(self[colname]) == "function" then
        -- todo : call the function with the arguments
      else
        self[colname] = self.cast(col.data_type, value)
      end
    else
        self[colname] = self.cast(col.data_type, value)
    end
    return true
  end
  return false
end

function inject(self, values)
  for k,v in pairs(values) do
    self:set(k, v)
  end
  return self
end

function new(self, db, tablename, values)
  o = {}
  setmetatable(o, self)
  self.__index = self
  self.tablename = tablename
  self.db = db
  self.info = db:tableinfo(tablename)
  self.__call = function(self, values)
    return self:inject(values)
  end
  if values then
    o:inject(values)
  end
  return o
end

function cast(ctype, value)
  if value ~= nil then
    if ctype == 'integer' then
      value = math.floor(value + 0.5)
    elseif ctype == 'float' then
      value = tonumber(value)
    else
      value = tostring(value)
    end
  end
  return value
end

function colinfo(self, colname)
  if type(colname) == "number" then
    return self.info.cols[colname]
  end
  for _, c in pairs(self.info.cols) do
    if c.column == colname then
      return c
    end
  end
  return false
end

