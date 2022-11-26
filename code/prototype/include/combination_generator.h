#ifndef LRC_COMBINATION_GENERATOR_H
#define LRC_COMBINATION_GENERATOR_H

#include "devcommon.h"
#include <random>
#include <utility>

void shuffle_helper(std::vector<int> & comb)
{
    std::default_random_engine dre(rand());
    std::uniform_int_distribution<int> uid(0,comb.size()-1);
    int from = uid(dre);
    int to = uid(dre) ;
    while(from==to) to=uid(dre);
    std::swap(comb[from],comb[to]);


}
std::vector<int> Vector_Add(const std::vector<int>& v,int inc,int base)
{
    std::vector<int> ret(v);
    int len=v.size();
    int carry=inc;
    for (size_t i = len; i < len; i++)
    {
        int pre = ret[i];
        ret[i]=(ret[i]+carry)%base;
        carry = (pre+carry)/base;
    }
    if(carry) ret.push_back(carry);
    std::reverse(ret.begin(),ret.end());
    return ret;
}


enum class ORDER{
    INC,DEC,AND
};
class combination_generator
{
private:
    // [mstart,mend] take count numbers without replacement
    int m_start;
    int m_end;
    int m_count;
    std::vector<int> prev;
    std::vector<int> cur;
    std::vector<int> next;
    ORDER order;
    bool isrolling;
public:
    combination_generator(int p_start,int p_end,int p_count):
            m_start(p_start),m_end(p_end),m_count(p_count),
            prev(std::vector<int>(m_count,-1)),cur(prev),next(cur),
            order(ORDER::INC),isrolling(false)
    {
#ifdef DEBUG
        assert(m_end-m_start+1>=m_count);
#endif
        std::vector<bool> occurance(m_end-m_start+1,false);
        int shift = m_start;
        int up = m_end-m_start;//[0,1,...,up]
        for (size_t i = 0; i < m_count; i++)
        {
            next[i]=i;
            occurance[i]=true;
        }
        prev.assign(up+1,-1);
        cur.assign(prev.cbegin(),prev.cend());
        // cur[k]=up-(m_count-1-k)

        int k = cur.size()-1;
        while(k>=0&&next[k]==up+1+k-m_count) k--;//find first pos increasible
        if(k<0) //max
        {
            isrolling=true;
            return ;
        }else{
            next[k++]++;
            while(k<cur.size())
            {
                next[k]=next[k-1]+1;
                k++;
            }
        }
    }

    bool IsRolling() const{ return isrolling; }

    std::pair<std::vector<int>,bool> Generate()
    {
        //1.prev=cur
        //2.cur=next
        //3.next go ahead
        std::vector<int> tmp(next.cbegin(),next.cend());
        int k = tmp.size()-1;
        while(k>=0&&tmp[k]==m_end-m_start+1+k-m_count) k--;//find first pos increasible
        if(k<0) //max
        {
            isrolling=true;
            for(int i=0;i<m_count;++i)
            {
                next[i]=i;
            }
            prev.assign(cur.cbegin(),cur.cend());
            cur.assign(tmp.cbegin(),tmp.cend());
            return {tmp,isrolling};
        }else{
            next[k++]++;
            while(k<cur.size())
            {
                next[k]=next[k-1]+1;
                k++;
            }
            prev.assign(cur.cbegin(),cur.cend());
            cur.assign(tmp.cbegin(),tmp.cend());
            return {tmp,isrolling};
        }
    }

    std::vector<int> ShowCombination() const
    {
        //plus m_start
        int shift=m_start;
        std::vector<int> ret(cur.cbegin(),cur.cend());
        std::for_each(ret.begin(),ret.end(),[shift](int & e){ e+=shift;});
        return ret;
    }
    ~combination_generator(){}
};

#endif //LRC_COMBINATION_GENERATOR_H
