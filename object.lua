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
    if self.__exists and col.key and self.__values[colname] then
      -- store older primary key for update
      if not self.__previously then self.__previously = {} end
      if not self.__previously[colname] then
        self.__previously[colname] = self.__values[colname]
      end
    end
    local value = self.__db:cast(col.data_type, value)
    if not self.__values[colname] or value ~= self.__values[colname] then
      self.__values[colname] = value
      self.__dirty = true
    end
    return true
  end
  return false
end

function value(self, colname)
  if self.__values then
    return self.__values[colname]
  end
end

function values(self, return_type)
  if not self.__values then
    return {}
  end
  return self.__values
end

function link(self, obj)
  local relation = assert(self.__db:getRelation(self.__table, obj.__table), "relations between ".. self.__table .. " and " .. obj.__table .. " must be defined in module")  
  obj:freeze()
  relation:link(self, obj)
end

function getOne(self, table2, options, ...)
  local values = {...}
  local options = options or {}
  local relation = assert(self.__db:getRelation(self.__table, table2), "relations between ".. self.__table .. " and " .. table2 .. " must be defined in module")
  relation:prepare(self, table2, options, values)
  local obj = self.__db:findOne(table2, options, unpack(values))
  return obj
end

function getAll(self, table2, options, ...)
  local values = {...}
  local options = options or {}
  local relation = assert(self.__db:getRelation(self.__table, table2), "relations between ".. self.__table .. " and " .. table2 .. " must be defined in module")
  relation:prepare(self, table2, options, values)
  local objs = self.__db:findAll(table2, options, unpack(values))
  return objs
end

function trigger(self, func)
  if self[func] and type(self[func]) == "function" then
    return self[func](self)
  end
end

function freeze(self)
  if not self.__exists then
    self:insert()
  end
  if self.__dirty then
    self:update()
  end
  -- freeze relations, cascade ?

  return self
end

function insert(self)
  self:trigger("onInsert")
  local info = self:info()
  local query = "INSERT INTO " .. self.__db:identifier(self.__table) .. " ("
  local cols = {}
  local vals = {}
  for k,col in ipairs(info.cols) do
    if self.__values[col.column] then
      cols[#cols+1] = self.__db:identifier(col.column)
      vals[#vals+1] = "?"
    elseif col.null then
      cols[#cols+1] = self.__db:identifier(col.column)
      vals[#vals+1] = "NULL"
    end
  end
  query = query .. table.concat(cols, ", ") .. ") VALUES ("
  query = query .. table.concat(vals, ", ") .. ")"

  local stmt = self.__db:prepare(query)
  local values = {}
  for i,col in ipairs(info.cols) do
    if self.__values[col.column] then
      values[#values+1] = self.__values[col.column]
    end
  end
  self.__db:execute(stmt, values)
  if info.autoinc then
    local last_id = self.__db:lastInsertId()
    self:setValue(info.pk[1], last_id)
  end
  self.__exists = true
  self.__dirty = false
  self:trigger("onInserted")
  return self
end


function update(self)
  self:trigger("onUpdate")
  local info = self:info()
  local query = "UPDATE " .. self.__db:identifier(self.__table) .. " SET "
  local cols = {}
  local values = {}
  local where = {}
  for k,col in ipairs(info.cols) do
    if self.__values[col.column] then
      cols[#cols+1] = self.__db:identifier(col.column) .. " = ?"
      values[#values+1] = self.__values[col.column]
    elseif col.null then
      cols[#cols+1] = self.__db:identifier(col.column) .. " = NULL"
    end
  end
  query = query .. table.concat(cols, ", ") .. " WHERE "
  for _, pk in ipairs(info.pk) do
    where[#where+1] = self.__db:identifier(pk) .. " = ?"
    if self.__previously and self.__previously[pk] then
      values[#values+1] = self.__previously[pk]
    else
      values[#values+1] = self.__values[pk]
    end
  end
  query = query .. table.concat(where, " AND ")
  local stmt = self.__db:prepare(query)
  self.__db:execute(stmt, values)
  self.__dirty = false
  self.__previously = nil
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
        table.insert(r, tab:value(k))
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


