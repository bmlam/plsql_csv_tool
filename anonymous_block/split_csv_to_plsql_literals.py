#! /c/Users/user1/AppData/Local/Programs/Python/Python37-32/python3

""" This program reads a the content of a CSV file and split it into chunks of PLSQL literals,
as ORA_MINING_VARCHAR2_NT
The intention is that this PLSQL collection can be picked up by PLSQL code which for example will 
insert the delimiter separated list of columns into a database table 
"""

import argparse, inspect, os, sys, tempfile 

g_path_separator = "\\"
g_internalSepator = ":"

def dosPath2Unix( inp ):
  outp = inp.replace( "C:\\" , "/c/" )
  return  outp.replace( "\\", r"/" )

def parseCmdLine() :
  global g_debugOn

  parser = argparse.ArgumentParser()
  # lowercase shortkeys
  parser.add_argument( '-i', '--inputFile', help='path to the file', required= True )
  # parser.add_argument( '-s', '--sep', help='separator', default= g_unknownFeature , default = ";" )
  parser.add_argument( '--debug', help='print debugging messages', required= False, action='store_true', default= False )

  # long keywords only
  # parser.add_argument( '--batch_mode', dest='batch_mode', action='store_true', help= "Run in batch mode. Interactive prompts will be suppressed" )

  result= parser.parse_args()
  g_debugOn = result.debug

  return result

def _dbx( msg ):
  global g_debugOn
  if g_debugOn:
    print( "dbx: %s" % (msg) ) 

def main(): 
  literalTemplate = """q'[{line}]' 
"""
  homeLocation = os.path.expanduser( "~" )
  cmdLnConfig = parseCmdLine()
  
  fh = open( cmdLnConfig.inputFile, "r" )
  csvLines = fh.readlines()
  _dbx( "lines read: %d" % len( csvLines ) ) 
  fh.close()

  LITERAL_MAX_SIZE = 4000 ; SAFETY_GAP = 2; LINEBREAK_SIZE = 2 
  chunks = []
  resetChunk = True 
  headerLine = ""
  for ix, line in enumerate( csvLines ):
    if ix == 0: 
      headerLine = line.rstrip( "\n" ) 
      continue   # hold back this from first chunk as we will add header line for all chunks 

    if resetChunk:
      linesInChunk = []; currChunkLen = 0
      linesInChunk.append( headerLine + " ") 

    _dbx( "lineNo: %d currChunkLen: %d " % ( ix, currChunkLen ) )
    if currChunkLen + len( line ) + SAFETY_GAP + LINEBREAK_SIZE < LITERAL_MAX_SIZE:
      # buffer line in chunk 
      linesInChunk.append(line.rstrip("\n") + " ")
      currChunkLen += len( line ) + LINEBREAK_SIZE 
      resetChunk = False 
    else:
      if ix < 500: 
        _dbx( "lineNo: %d len of linesInChunk:%d" % ( ix, len(linesInChunk) ) )

      # finalize current chunk 
      chunk = literalTemplate.format( line= "\n".join( linesInChunk ) ) 
      chunks.append( chunk )

      resetChunk = True 

      if len( chunks ) == 1:
        _dbx( "first chars of chunk:\n%s" % ( chunk[0:100] ) )
      # f ix > 12000: break 
  
  outFile = tempfile.mkstemp( suffix= ".sql" )[1]

  fh = open( outFile, "w" )
  fh.write( "  ORA_MINING_VARCHAR2_NT (" )
  for ix, chunk in enumerate( chunks ):
    if ix % 5 == 0 : # add a position marker every N chunks 
      fh.write( "/* chunk %d */" % (ix + 1) )
    if ix == 0 :
      fh.write( chunk )
    else:
      fh.write( ' , ' + chunk )
    # if ix > 4: break 
  fh.write( ");" )

  print( "output can be found in %s. Unix style: %s" % ( outFile, dosPath2Unix(outFile) ) ) 

if __name__ == "__main__" : 
  main()