package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

var (
	URL                        = "https://cb3bp-ciaaa-aaaai-qkw4q-cai.raw.ic0.app"
	STATE_PATH                 = "state.json"
	NNS_POLL_INTERVALL         = 5 * time.Minute
	STATE_PERSISTENCE_INTERVAL = 5 * time.Minute
	MAX_TOPIC_LENGTH           = 50
	MAX_BLOCKED_TOPICS         = 30
	MAX_SUMMARY_LENGTH         = 2048
	TOPIC_GOVERNANCE           = "Governance"
	ALL_EXCEPT_GOVERNANCE      = "AllButGovernance"
)

type Proposal struct {
	Title    string `json:"title"`
	Topic    string `json:"topic"`
	Id       uint64 `json:"id"`
	Summary  string `json:"summary"`
	Proposer uint64 `json:"proposer"`
}

type State struct {
	LastSeenProposal uint64                    `json:"last_seen_proposal"`
	ChatIds          map[int64]map[string]bool `json:"chat_ids"`
	lock             sync.RWMutex
}

// Locks the state, persists it to a temporary file, then moves the temporary
// file to the location of the persisted state. This should avoid broken state
// if the process gets killed in the middle of writing.
func (s *State) persist() {
	s.lock.RLock()
	data, err := json.Marshal(s)
	s.lock.RUnlock()
	if err != nil {
		log.Println("Couldn't serialize state:", err)
		return
	}
	tmpFile, err := ioutil.TempFile(".", STATE_PATH+"_tmp_")
	if err != nil {
		log.Fatal(err)
	}
	err = os.WriteFile(tmpFile.Name(), data, 0644)
	if err != nil {
		log.Println("Couldn't write to state file", STATE_PATH, " :", err)
	}
	os.Rename(tmpFile.Name(), STATE_PATH)
	log.Println(len(data), "bytes persisted to", STATE_PATH)
}

// Deserialize the persisted state from the disk. Currently, prints an error on a first run.
func (s *State) restore() {
	data, err := os.ReadFile(STATE_PATH)
	if err != nil {
		log.Println("Couldn't read file", STATE_PATH)
	}
	if err := json.Unmarshal(data, &s); err != nil {
		log.Println("Couldn't deserialize the state file", STATE_PATH, ":", err)
	}
	if s.ChatIds == nil {
		s.ChatIds = map[int64]map[string]bool{}
	}
	fmt.Println("Deserialized the state with", len(s.ChatIds), "users, last proposal id:", s.LastSeenProposal)
}

// This is an atomic compare and swap for a new seen proposal id.
func (s *State) setNewLastSeenId(id uint64) (updated bool) {
	s.lock.Lock()
	if s.LastSeenProposal < id {
		s.LastSeenProposal = id
		updated = true
	}
	s.lock.Unlock()
	return
}

// Unsubscribes the chat id.
func (s *State) removeChatId(id int64) {
	s.lock.Lock()
	delete(s.ChatIds, id)
	s.lock.Unlock()
	log.Println("Removed user", id, "from subscribers")
}

// Subscribes the chat id.
func (s *State) addChatId(id int64) {
	s.lock.Lock()
	s.ChatIds[id] = map[string]bool{}
	s.lock.Unlock()
	log.Println("Added user", id, "to subscribers")
}

// Block `topic` for chat `id`. Checks max topic length and max blocked topics to avoid
// trivial bloat attacks.
func (s *State) blockTopic(id int64, topic string) {
	if len(topic) > MAX_TOPIC_LENGTH {
		return
	}
	s.lock.Lock()
	blacklist := s.ChatIds[id]
	if blacklist != nil && len(blacklist) < MAX_BLOCKED_TOPICS {
		blacklist[topic] = true
	}
	s.lock.Unlock()
}

// Unblocks `topic` for chat `id`.
func (s *State) unblockTopic(id int64, topic string) {
	s.lock.Lock()
	blacklist := s.ChatIds[id]
	if blacklist != nil {
		delete(blacklist, topic)
	}
	s.lock.Unlock()
}

// Returns the list of chat ids which should be notified about `topic`.
func (s *State) chatIdsForTopic(topic string) (res []int64) {
	s.lock.RLock()
	for id, blacklist := range s.ChatIds {
		// Skip if no blacklist or topic is blacklisted.
		if blacklist == nil || blacklist[topic] {
			continue
		}
		// Skip if only governance topic is whitelisted and the topic is not governance.
		if blacklist[ALL_EXCEPT_GOVERNANCE] && topic != TOPIC_GOVERNANCE {
			continue
		}
		res = append(res, id)
	}
	s.lock.RUnlock()
	return
}

// Returns a string of blocked topics.
func (s *State) blockedTopics(id int64) string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	m := s.ChatIds[id]
	if m == nil || len(m) == 0 {
		return "Your list of blocked topics is empty."
	}
	var res []string
	for topic, enabled := range m {
		if enabled {
			res = append(res, topic)
		}
	}
	return fmt.Sprintf("You've blocked these topics: %s.", strings.Join(res, ", "))
}

func main() {
	bot, err := tgbotapi.NewBotAPI(os.Getenv("TOKEN"))
	if err != nil {
		log.Panic("Couldn't instantiate the bot API:", err)
	}

	log.Printf("Authorized on account %s", bot.Self.UserName)
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	var state State
	state.restore()

	go fetchProposalsAndNotify(bot, &state)
	go persist(&state)

	updates := bot.GetUpdatesChan(u)
	for update := range updates {
		if update.Message == nil {
			continue
		}
		var msg string
		id := update.Message.Chat.ID
		words := strings.Split(update.Message.Text, " ")
		if len(words) == 0 {
			continue
		}
		cmd := words[0]
		switch cmd {
		case "/start":
			state.addChatId(id)
			msg = "Subscribed." + "\n\n" + getHelpMessage()
		case "/stop":
			state.removeChatId(id)
			msg = "Unsubscribed."
		case "/block", "/unblock":
			if len(words) != 2 {
				msg = fmt.Sprintf("Please specify one topic")
				break
			}
			topic := words[1]
			switch cmd {
			case "/block":
				state.blockTopic(id, topic)
			default:
				state.unblockTopic(id, topic)
			}
			msg = state.blockedTopics(id)
		case "/governance_only":
			state.blockTopic(id, ALL_EXCEPT_GOVERNANCE)
			msg = "From now on, you'll only see the governance proposals."
		case "/blacklist":
			msg = state.blockedTopics(id)
		default:
			msg = getHelpMessage()
		}
		bot.Send(tgbotapi.NewMessage(id, msg))
	}
}

func getHelpMessage() string {
	return "Enter /stop to unsubscribe (/start to resubscribe). " +
		"Use /block or /unblock to block or unblock proposals with a certain a topic; " +
		"use /blacklist to display the list of blocked topics. " +
		"Use /governance_only command to only receive governance proposals."
}

func persist(state *State) {
	ticker := time.NewTicker(STATE_PERSISTENCE_INTERVAL)
	for range ticker.C {
		state.persist()
	}
}

func fetchProposalsAndNotify(bot *tgbotapi.BotAPI, state *State) {
	ticker := time.NewTicker(NNS_POLL_INTERVALL)
	for range ticker.C {
		resp, err := http.Get(URL)
		if err != nil {
			log.Println("GET request failed from", URL, ":", err)
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println("Couldn't read the response body:", err)
		}

		var proposals []Proposal
		if err := json.Unmarshal(body, &proposals); err != nil {
			fmt.Println("Couldn't parse the response as JSON:", err)
			continue
		}

		sort.Slice(proposals, func(i, j int) bool { return proposals[i].Id < proposals[j].Id })

		for _, proposal := range proposals {
			if !state.setNewLastSeenId(proposal.Id) {
				continue
			}
			log.Println("New proposal detected:", proposal)
			summary := proposal.Summary
			if len(summary)+2 > MAX_SUMMARY_LENGTH {
				summary = "[Proposal summary is too long.]"
			}
			if len(summary) > 0 {
				summary = "\n" + summary + "\n"
			}
			text := fmt.Sprintf("<b>%s</b>\n\nProposer: %d\n%s\n#%s\n\nhttps://nns.ic0.app/proposal/?proposal=%d",
				proposal.Title, proposal.Proposer, summary, proposal.Topic, proposal.Id)

			ids := state.chatIdsForTopic(proposal.Topic)
			for _, id := range ids {
				msg := tgbotapi.NewMessage(id, text)
				msg.ParseMode = tgbotapi.ModeHTML
				msg.DisableWebPagePreview = true
				_, err := bot.Send(msg)
				if err != nil {
					log.Println("Couldn't send message:", err)
					if strings.Contains(err.Error(), "bot was blocked by the user") {
						state.removeChatId(id)
					}
				}
			}
			if len(ids) > 0 {
				log.Println("Successfully notified", len(ids), "users")
			}
		}
	}
}
