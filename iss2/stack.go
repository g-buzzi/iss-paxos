package iss2

type Stack struct {
	stack []interface{}
}

func (s Stack) len() int {
	return len(s.stack)
}

func newStack() Stack {
	return Stack{stack: make([]interface{}, 0)}
}

func (s *Stack) add(el interface{}) {
	s.stack = append(s.stack, el)
}

func (s *Stack) pop() interface{} {
	n := len(s.stack)
	if n == 0 {
		return nil
	}
	el := s.stack[0]
	s.stack = s.stack[1:n]
	return el
}
