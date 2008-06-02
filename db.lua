
module('frigo.db', package.seeall)

_COPYRIGHT = "Copyright (C) 2008 Bertrand Mansion"
_DESCRIPTION = "Frigo module"
_VERSION = "0.0.1"

prepared_queries = {}
last_query = ""

function connect(self, driver, database, username, password, ...)
  assert(driver, "driver required to make luasql connection")
  local adapter = require("frigo.adapter." .. driver)
  local c = adapter:connect(database, username, password, ...)
  setmetatable(c, self)
  self.__index = self
  self.__tostring = self.tostring
  return c
end

function close(self)
	self.conn:close()
	self.conn = nil
	self.last_query = ""
	self:freePrepared()
	setmetatable(self, nil)
end

function tostring(self)
  local info = string.lower(self._NAME) .. ": (driver="..self.driver..")"
  if self.conn then
    info = info .. " [connected]"
  end
  return info
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

  local rows = cursor:fetch({}, mode)
  if not rows then return nil end
  if not rows[col] then error("no such field") end

  local i = 0
  local results = {}
	while rows do
	  i = i + 1
    results[i] = rows[col]
    rows = cursor:fetch({}, mode)
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
    local rows = cursor:fetch({}, "n")
	  while rows do
	    if group then
	      if not results[rows[1]] then
	        results[rows[1]] = {}
	      end
        table.insert(results[rows[1]], rows[2])
      else
        results[rows[1]] = rows[2]
      end
      rows = cursor:fetch({}, "n")
	  end
	  cursor:close()
	  return results
  else
    local results = {}
    local rows = cursor:fetch({}, "n")
	  while rows do
	    if group then
	      if not results[rows[1]] then
	        results[rows[1]] = {}
	      end
	      if mode == "a" then
	        local row = {}
	        for i = 2, #rows do
	          row[cols[i]] = rows[i]
	        end
          table.insert(results[rows[1]], row)
	      else
	        local row = {}
	        for i = 2, #rows do
	          table.insert(row, rows[i])
	        end
          table.insert(results[rows[1]], row)
	      end
      else
	      if not results[rows[1]] then
	        results[rows[1]] = {}
	      end
	      if mode == "a" then
	        local row = {}
	        for i = 2, #rows do
	          row[cols[i]] = rows[i]
	        end
          results[rows[1]] = row
	      else
	        local row = {}
	        for i = 2, #rows do
	          table.insert(row, rows[i])
	        end
          results[rows[1]] = row
	      end
      end
      rows = cursor:fetch({}, "n")
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
  local rows = cursor:fetch({}, mode)
  local i = 0
	while rows do
	  i = i + 1
    results[i] = rows
    rows = cursor:fetch({}, mode)
	end
	cursor:close()
  return results
end

function setautocommit(self, onoff)
  self.conn:setautocommit(onoff)
end

function commit(self)
  self.conn:commit()
end

function rollback(self)
  self.conn:rollback()
end

function getSequenceName(sqn)

end

function nextid(seqname, ondemand)

end

function createSequence(seqname)
  
end

function dropSequence(seqname)
  
end

function tablelist(self)
  error("not implemented")
end

function tableinfo(self, tablename)
  error("not implemented")
end