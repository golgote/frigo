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

function setvalue(self, colname, value)
  if not self._values then
    self._values = {}
  end
  local col = self:colinfo(colname)
  if col then
    self._values[colname] = self.cast(col.data_type, value)
    return true
  end
  return false
end

function primarykey()
  
end

function set()
  
end

function add()
  
end

function get()
  
end

function trigger()
  
end

function freeze()
  
end

function insert()
  
end

function update()
  
end

function delete()
  
end

function clone()
  
end



function getvalue(self, colname)
  if self._values then
    return self._values[colname]
  end
end

function getvalues(self)
  return self._values or {}
end

function info(self)
  return self._db:tableinfo(self.tablename)
end

function new(self, db, o)
  local o = o or {}
  o._db = db
  self.__index = self
  self.__call = function(tab, value)
    if value[1] then
      local r = {}
      for _,k in ipairs(value) do
        table.insert(r, tab:getvalue(k))
      end
      return unpack(r)
    else
      for k,v in pairs(value) do
        tab:setvalue(k, v)
      end
      return tab
    end
  end
  setmetatable(o, self)
  local info = o:info()
  for _, c in pairs(info.cols) do
    if c.default then
      o:setvalue(c.column, c.default)
    end
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
  local info = self:info()
  if type(colname) == "number" then
    return info.cols[colname]
  end
  for _, c in pairs(info.cols) do
    if c.column == colname then
      return c
    end
  end
  return false
end

