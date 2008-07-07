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


function setValue(self, colname, value)
  local col = self:colinfo(colname)
  if col then
    self.__values[colname] = self.cast(col.data_type, value)
    self.__dirty = true
    return true
  end
  return false
end

function getValue(self, colname)
  if self.__values then
    return self.__values[colname]
  end
end

function getValues(self)
  return self.__values or {}
end

function set(self, obj)
  
end

function add(self, obj)
  if not self.__exists then
    -- starts by saving the object if not saved

  end
  if obj.__exists then
    -- loaded object
    --self.__db:cached()
  else
    -- loaded object
    --relation:link(self, obj)
  end
end

function getOne(self, table2, options, ...)
  local values = {...}
  local options = options or {}
  local relation = assert(self.__db:getRelation(self.__table, table2), "relations between ".. self.__table .. " and " .. table2 .. " must be defined in module")  
  relation:prepare(self, table2, options, values)
  local obj = self.__db:findOne(table2, options, unpack(values))
  return obj
end

function getAll(self, tablename, options, ...)
  -- local obj = self.__db:findMany(tablename, options, unpack(values))  
end

function trigger(self, func)
  
end

function freeze(self)
  
end

function insert(self)
  self:trigger("onInsert")


  self:trigger("onInserted")
  return self
end


function update(self)
  self:trigger("onUpdate")


  self:trigger("onUpdated")
  return self
end

function delete(self)
  self:trigger("onDelete")


  self:trigger("onDeleted")
  return self
end

function clone(self)
  
end

function info(self)
  return self.__db:tableinfo(self.__table)
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

function new(self, db, o)
  local obj
  if type(o) == "table" then
    obj = o
  elseif type(o) == "string" then
    obj = {}
    obj.__table = o
  end

  assert(obj.__table, "frigo object requires a '__table' field")
  assert(db:tableExists(obj.__table), "table '" .. obj.__table .. "' not found")

  local model = db:preload(obj.__table)
  for k, v in pairs(model) do
    if k ~= "_M" then
      obj[k] = v
    end
  end

  obj.__db = db
  obj.__values = {}
  obj.__exists = false
  self.__index = self
  self.__call = function(tab, value)
    if value[1] then
      local r = {}
      for _,k in ipairs(value) do
        table.insert(r, tab:getValue(k))
      end
      return unpack(r)
    else
      for k,v in pairs(value) do
        tab:setValue(k, v)
      end
      return tab
    end
  end
  setmetatable(obj, self)
  local info = obj:info()
  for _, c in pairs(info.cols) do
    if c.default then
      obj:setValue(c.column, c.default)
    end
  end
  obj.__dirty = false
  obj:trigger("onInit")
  return obj
end


