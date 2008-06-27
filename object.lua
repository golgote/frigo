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
  local col = self:colinfo(colname)
  if col then
    self.__values[colname] = self.cast(col.data_type, value)
    self.__dirty = true
    return true
  end
  return false
end

function set(self)
  
end

function add(self)
  
end

function getRelation(self, tablename)
--[[  if not self.relations then
    return nil
  end
  -- todo : cache constructed relations
  for _, tab in pairs(self.relations) do
    if #tab > 3 then
      -- n:n

    else
      -- 1:1 or 1:n

    end
  end
  local relation = {
    from = "",
    to = "",
    prepareSelect = function(self, options, values)
      
    end,
  }
  return relation]]
end

function getOne(self, tablename, options, ...)
--[[  local info = self:info()
  for _, colname in pairs(info.pk) do
    if not self.__values[colname] then
      return
    end
  end

  local values = ...
  local loaded = true;
  local relation = self:getRelation(tablename)
  if not relation then
    error("no relation between ".. self.__table .. " and " .. tablename)
  end

  relation:prepareSelect(options, values)

  local obj = self.__db:findOne(tablename, options, unpack(values))
  -- local obj = self.__db:findMany(tablename, options, unpack(values))

]]
end

function getMany(self, tablename, options, ...)
  
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

function getvalue(self, colname)
  if self.__values then
    return self.__values[colname]
  end
end

function getvalues(self)
  return self.__values or {}
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

  -- load object if found

  local path = package.path
  if db.options["path"] then
    package.path = db.options["path"]
  end
  local prefix = ""
  if db.options["prefix"] then
    prefix = db.options["prefix"] .. "."
  end
  local mod = prefix .. obj.__table

  local status, model = nil, {}
  if not db.notfound[mod] then
    status, model = pcall(require, mod)
    if not status then
      db.notfound[mod] = true
      model = {}
    else
      for k, v in pairs(model) do
        if k ~= "_M" then
          obj[k] = v
        end
      end
    end
  end
  package.path = path

  obj.__db = db
  obj.__values = {}
  obj.__loaded = false
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
  setmetatable(obj, self)
  local info = obj:info()
  for _, c in pairs(info.cols) do
    if c.default then
      obj:setvalue(c.column, c.default)
    end
  end
  obj.__dirty = false
  obj:trigger("onInit")
  return obj
end


