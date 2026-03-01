Overall task: Generate pods for a cedh tournament

Cedh tournament orgization:

Pods:
Players are grouped into pods of 4 players. If there are three players left then a pod of three players will be generated. If there are two or fewer remainders, those players get a buy.

Tournament:
The tournament is in a round robin format where each player plays against as many different people as possible.

Files - Input: tournament_input.txt

Files - Output: tournament_output.gsheet
    - this file will display the players grouped by pod for each game
    - this file will have a seperate section noting the total amount of duplicate machups per player
    - ie if player a sees player b in more than one pod then that would be a duplicate matchup regarless of the uniqueness of the other two players in the matchup

Task: pod generation
    - read the input file and generate pods to be stuck into the output file
    - try to keep the pods as unique as possible, no two players should play against each other more than twice
