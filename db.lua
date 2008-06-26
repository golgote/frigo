-------------------------------------------------------------------------------
-- Frigo is a simple ORM working on top of LuaSQL.
--
-- @author Bertrand Mansion (bmansion@mamasam.com)
-- @copyright 2008 Bertrand Mansion
-------------------------------------------------------------------------------

module('frigo.db', package.seeall)

_COPYRIGHT = "Copyright (C) 2008 Bertrand Mansion"
_DESCRIPTION = "Frigo DB is a simple wrapper for common LuaSQL manipulations."
_VERSION = "0.0.1"

function new(driver, options)
  assert(driver, "driver required to make luasql connection")

  local options = options or {}
  local connection = {
    driver = driver,
    prepared_queries = {},
    last_query = "",
    infocache = {},
    notfound = {},
    options = options
  }

  local adapter = require("frigo.adapter." .. driver)
  for k, v in pairs(adapter) do
    if k ~= "_M" then
      connection[k] = v
    end
  end

  setmetatable(connection, {__index = _M})
  return connection
end

function connect(self, database, username, password, ...)
  local luasql = require("luasql." .. self.driver)
  local env, err = luasql[self.driver]()
  if not env then
    error(err)
  end

  local conn, err = env:connect(database, username, password, ...)
  if not conn then 
    error(err)
  end
  self.conn = conn
  return self
end

function close(self)
	self.conn:close()
	self.conn = nil
	self.last_query = ""
	self:freePrepared()
	setmetatable(self, nil)
end

function identifier(self, str)
  return '"' .. str:gsub('"', '""') .. '"'
end

function escape(self, str)
  return str:gsub("'", "''")
end

function quote(self, val)
  if type(val) == "number" then
    if string.find(val, "[%.,]") then
      return "'" .. string.gsub(val, ",", ".") .. "'"
    else
      return val
    end
  elseif type(val) == "nil" then
    return 'NULL'
  elseif type(val) == "boolean" then
    return val and "1" or "0";
  else
    return "'" .. self.escape(self, val) .. "'"
  end
end

function prepare(self, q)
  local tokens = {}
  q:gsub("([^%?]*)%?", function(c) table.insert(tokens, c) end)
  if #tokens > 0 then
    q:gsub("%?([^%?]*)$", function(c) table.insert(tokens, c) end)
    table.insert(self.prepared_queries, tokens)
  else
    table.insert(self.prepared_queries, q)
  end
  return #self.prepared_queries
end

function execute(self, stmt, ...)
  if type(stmt) == "string" then
    last_query = stmt
  else
    last_query = self:buildQuery(stmt, ...)
  end
  local cursor, msg = assert(self.conn:execute(last_query))
	return cursor or error(msg .. " SQL = { " .. last_query .. " }", 2)
end

function buildQuery(self, stmt, ...)
  local stmt = assert(self.prepared_queries[stmt], "prepared statement not found")
  local count = select("#", ...)
  local query = ""
  if count > 0 then
    if type(stmt) == "string" or (#stmt-1) ~= count then
      error("prepared statement expected " ..
        #stmt .. " values, got " .. count)
    end
    for i = 1, count do
      local value = self:quote(select(i, ...))
      query = query .. stmt[i] .. value
    end
    if stmt[count+1] then
      query = query .. stmt[count+1]
    end
    return query
  else
    return stmt
  end
end

function freePrepared(self, stmt)
  if not stmt then
    self.prepared_queries = {}
  else
    self.prepared_queries[stmt] = nil
  end
end

function limitQuery(self, q, from, count, ...)
  return q
end

function getOne(self, q, ...)
  local stmt = self:prepare(q)
  local cursor = self:execute(stmt, ...)
  self:freePrepared(stmt)

  local row = cursor:fetch({}, "n")
	cursor:close()
	if row then
	  return row[1]
  else
    return nil
  end
end

function getRow(self, q, mode, ...)
  local stmt = self:prepare(q)
  local cursor = self:execute(stmt, ...)
  self:freePrepared(stmt)

  local row = cursor:fetch({}, mode)
	cursor:close()
  return row
end

function getCol(self, q, col, ...)
  local stmt = self:prepare(q)
  local cursor = self:execute(stmt, ...)
  self:freePrepared(stmt)

  local mode = "a"
  if type(col) == "number" then mode = "n" end

  local row = cursor:fetch({}, mode)
  if not row then return nil end
  if not row[col] then error("no such field") end

  local i = 0
  local results = {}
	while row do
	  i = i + 1
    results[i] = row[col]
    row = cursor:fetch({}, mode)
	end
	cursor:close()
  return results
end

function getAssoc(self, q, group, mode, ...)
  local stmt = self:prepare(q)
  local cursor = self:execute(stmt, ...)
  self:freePrepared(stmt)

  local cols = cursor:getcolnames()
  if #cols < 2 then
	  cursor:close()
    error("truncated")
  elseif #cols == 2 then
    local results = {}
    local row = cursor:fetch({}, "n")
	  while row do
	    if group then
	      if not results[row[1]] then
	        results[row[1]] = {}
	      end
        table.insert(results[row[1]], row[2])
      else
        results[row[1]] = row[2]
      end
      row = cursor:fetch({}, "n")
	  end
	  cursor:close()
	  return results
  else
    local results = {}
    local row = cursor:fetch({}, "n")
	  while row do
	    if group then
	      if not results[row[1]] then
	        results[row[1]] = {}
	      end
	      if mode == "a" then
	        local r = {}
	        for i = 2, #row do
	          r[cols[i]] = row[i]
	        end
          table.insert(results[row[1]], r)
	      else
	        local r = {}
	        for i = 2, #row do
	          table.insert(r, row[i])
	        end
          table.insert(results[row[1]], r)
	      end
      else
	      if not results[row[1]] then
	        results[row[1]] = {}
	      end
	      if mode == "a" then
	        local r = {}
	        for i = 2, #row do
	          r[cols[i]] = row[i]
	        end
          results[row[1]] = r
	      else
	        local r = {}
	        for i = 2, #row do
	          table.insert(r, row[i])
	        end
          results[row[1]] = r
	      end
      end
      row = cursor:fetch({}, "n")
	  end
	  cursor:close()
	  return results
  end
end

function getAll(self, q, mode, ...)
  local stmt = self:prepare(q)
  local cursor = self:execute(stmt, ...)
  self:freePrepared(stmt)

  local results = {}
  local row = cursor:fetch({}, mode)
  local i = 0
	while row do
	  i = i + 1
    results[i] = row
    row = cursor:fetch({}, mode)
	end
	cursor:close()
  return results
end

function tableExists(self, tablename)
  local list = self:tablelist()
  if list[tablename] then return true end
  return false
end

function factory(self, o)
  if not self.notfound then
    self.notfound = {}
  end
  local object = require"frigo.object"
  return object:new(self, o)
end

function alias(self, tablename, colname)
  return tablename .. "." .. colname .. " AS " .. tablename .. '_' .. colname
end

function find(self, query, ...)
  local found = {}
  local aliases = {}
  local q = string.gsub(query, '{([%w_]+)%:?([^}]*)}', function(t, a) 
    table.insert(found, t)
    if a ~= "" then
      aliases[t] = a
    end
    local info = self:tableinfo(t)
    local replace = {}
    for _,col in pairs(info.cols) do
      local tab = aliases[t] or t
      local alias = self:alias(tab, col.column)
      table.insert(replace, alias)
    end
    return table.concat(replace, ", ")
  end
  )

  local stmt = self:prepare(q)
  local state = { cursor = self:execute(stmt, ...), row = {} }
  self:freePrepared(stmt)

  local iterator = function(state)
    state.row = state.cursor:fetch(state.row, "a")
    if not state.row then
      state.cursor:close()
    else
      local objs = {}
      for _, tablename in ipairs(found) do
        local info = self:tableinfo(tablename)
        local obj = self:factory{ __table = tablename }
        for _, col in pairs(info.cols) do
          local tab = aliases[tablename] or tablename
          obj:setvalue(col.column, state.row[tab .. "_" .. col.column])
        end
        table.insert(objs, obj)
      end

      if #objs > 1 then
        return objs
      else
        return objs[1]
      end
    end
  end

  return iterator, state
end

function findOne(self, tablename, options, ...)
  local options = options or {}
  local query = "SELECT {".. tablename .. "} FROM "
  if options.using then
    if string.find(query, "%s+[Uu][Ss][Ii][Nn][Gg]%s+") then
      query = query .. options.using
    else
      query = query .. self:identifier(tablename) .. ", " .. options.using
    end
  else
    query = query .. self:identifier(tablename)
  end
  if options.where then
    query = query .. " WHERE " .. options.where
  end
  if options.groupby then
    query = query .. " GROUP BY " .. options.groupby
  end
  if options.having then
    query = query .. " HAVING " .. options.having
  end
  if options.orderby then
    query = query .. " ORDER BY " .. options.orderby
  end
  query = self:limitQuery(query, 1)
  for obj in self:find(query, ...) do
    return obj
  end
end

function findAll(self, tablename, options, ...)
  local options = options or {}
  local query = "SELECT {".. tablename .. "} FROM "
  if options.using then
    if string.find(query, "%s+[Uu][Ss][Ii][Nn][Gg]%s+") then
      query = query .. options.using
    else
      query = query .. self:identifier(tablename) .. ", " .. options.using
    end
  else
    query = query .. self:identifier(tablename)
  end
  if options.where then
    query = query .. " WHERE " .. options.where
  end
  if options.groupby then
    query = query .. " GROUP BY " .. options.groupby
  end
  if options.having then
    query = query .. " HAVING " .. options.having
  end
  if options.orderby then
    query = query .. " ORDER BY " .. options.orderby
  end
  query = self:limitQuery(query, options.limit, options.offset)
  local objs = {}
  for obj in self:find(query, ...) do
    table.insert(objs, obj)
  end
  return objs
end

function findId(self, tablename, ...)
  local info = self:tableinfo(tablename)
  local pk = info.pk
  if #pk ~= select('#', ...) then
    error("number of arguments mismatch")
  end
  local where = {}
  for _, k in pairs(pk) do
    table.insert(where, self:identifier(k) .. " = ?")
  end
  return self:findOne(tablename, {where = table.concat(where, " AND ")}, ...)
end
